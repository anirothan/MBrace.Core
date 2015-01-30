﻿module internal MBrace.SampleRuntime.Config

open System
open System.Reflection
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol

open Nessos.Vagabond

open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Serialization

let private runOnce (f : unit -> 'T) = let v = lazy(f ()) in fun () -> v.Value

let mutable private localCacheStore = Unchecked.defaultof<ICloudFileStore>
let mutable private fileStore = Unchecked.defaultof<ICloudFileStore>
let mutable private inMemoryCache = Unchecked.defaultof<IObjectCache>
let mutable private serializer = Unchecked.defaultof<FsPicklerStoreSerializer>

/// vagabond, fspickler and thespian state initializations
let private _initRuntimeState () =
    let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

    // vagabond initialization
    VagabondRegistry.Initialize(ignoredAssemblies = [Assembly.GetExecutingAssembly()], loadPolicy = AssemblyLoadPolicy.ResolveAll)
    serializer <- new FsPicklerBinaryStoreSerializer()

    // thespian initialization
    Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(serializer.Pickler)
    Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
    TcpListenerPool.RegisterListener(IPEndPoint.any)

    // store initialization
    // TODO : implement task-parametric store configuration
    let globalStore = FileSystemStore.CreateSharedLocal()
    localCacheStore <- FileSystemStore.CreateUniqueLocal() :> ICloudFileStore
    inMemoryCache <- InMemoryCache.Create()
    fileStore <- FileStoreCache.Create(globalStore, localCacheStore, localCacheContainer = "cache")

/// runtime configuration initializer function
let initRuntimeState = runOnce _initRuntimeState
/// returns the local ip endpoint used by Thespian
let getLocalEndpoint () = initRuntimeState () ; TcpListenerPool.GetListener().LocalEndPoint
let getSerializer () = initRuntimeState () ; serializer
let getAddress() = initRuntimeState () ; sprintf "%s:%d" TcpListenerPool.DefaultHostname (TcpListenerPool.GetListener().LocalEndPoint.Port)

/// initializes store configuration for runtime
let getFileStoreConfiguration defaultDirectory = 
    initRuntimeState ()
    { 
        FileStore = fileStore ; 
        DefaultDirectory = defaultDirectory ; 
        Serializer = serializer :> ISerializer ; 
        Cache = Some inMemoryCache
    }

let getFileStore () = initRuntimeState () ; fileStore
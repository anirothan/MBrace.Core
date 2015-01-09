﻿module internal MBrace.SampleRuntime.Config

open System
open System.Reflection
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol

open Nessos.Vagrant

open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Runtime.Vagrant
open MBrace.Runtime.Serialization

let private runOnce (f : unit -> 'T) = let v = lazy(f ()) in fun () -> v.Value

let mutable private fileStore = Unchecked.defaultof<ICloudFileStore>
let mutable private atomProvider = Unchecked.defaultof<ICloudAtomProvider>

/// vagrant, fspickler and thespian state initializations
let private _initRuntimeState () =
    let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

    // vagrant initialization
    let ignoredAssemblies =
        let this = Assembly.GetExecutingAssembly()
        let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
        hset dependencies

    VagrantRegistry.Initialize(ignoreAssembly = ignoredAssemblies.Contains, loadPolicy = AssemblyLoadPolicy.ResolveAll)

    // thespian initialization
    Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(VagrantRegistry.Pickler)
    Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
    TcpListenerPool.RegisterListener(IPEndPoint.any)

    // store initialization
    FileStoreCache.RegisterLocalFileSystemCache()
    InMemoryCacheRegistry.SetCache (InMemoryCache.Create())
    fileStore <- FileStoreCache.CreateCachedStore(FileSystemStore.LocalTemp :> ICloudFileStore)
    atomProvider <- FileSystemAtomProvider.LocalTemp :> ICloudAtomProvider

/// runtime configuration initializer function
let initRuntimeState = runOnce _initRuntimeState
/// returns the local ip endpoint used by Thespian
let getLocalEndpoint () = initRuntimeState () ; TcpListenerPool.GetListener().LocalEndPoint
let getAddress() = initRuntimeState () ; sprintf "%s:%d" TcpListenerPool.DefaultHostname (TcpListenerPool.GetListener().LocalEndPoint.Port)

/// initializes store configuration for runtime
let getStoreConfiguration defaultDirectory atomContainer = 
    initRuntimeState ()
    resource {
        yield { FileStore = fileStore ; DefaultDirectory = defaultDirectory }
        yield { AtomProvider = atomProvider ; DefaultContainer = atomContainer }
        yield VagrantRegistry.Serializer
    }
    

let getFileStore () = initRuntimeState () ; fileStore
let getAtomProvider () = initRuntimeState () ; atomProvider
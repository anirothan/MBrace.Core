﻿namespace MBrace.SampleRuntime.Actors

//
//  Implements a collection of distributed resources that provide
//  coordination for execution in the distributed runtime.
//  The particular implementations are done using Thespian,
//  a distributed actor framework for F#.
//

open System
open System.Collections.Generic
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols

open Nessos.Vagabond

open MBrace
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Vagabond
open MBrace.SampleRuntime

/// Actor publication utilities
type Actor private () =
    static do Config.initRuntimeState()

    /// Publishes an actor instance to the default TCP protocol
    static member Publish(actor : Actor<'T>) =
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ Protocols.utcp() ]
        |> Actor.start

    /// Exception-safe stateful actor behavior combinator
    static member Stateful (init : 'State) f =
        let rec aux state (self : Actor<'T>) = async {
            let! msg = self.Receive()
            let! state' = async {
                try return! f state msg
                with e -> printfn "Actor fault (%O): %O" typeof<'T> e ; return state
            }

            return! aux state' self
        }

        Actor.bind (aux init)

    /// Exception-safe stateless actor behavior combinator
    static member Stateless (f : 'T -> Async<unit>) =
        Actor.Stateful () (fun () t -> f t)

//
//  Distributed latch implementation
//

type private LatchMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

/// Distributed latch implementation
type Latch private (source : ActorRef<LatchMessage>) =
    /// Atomically increment the latch
    member __.Increment () = source <!- Increment
    /// Returns the current latch value
    member __.Value = source <!= GetValue
    /// Initialize a new latch instance in the current process
    static member Init(init : int) =
        let behaviour count msg = async {
            match msg with
            | Increment rc ->
                do! rc.Reply (count + 1)
                return (count + 1)
            | GetValue rc ->
                do! rc.Reply count
                return count
        }

        let ref =
            Actor.Stateful init behaviour
            |> Actor.Publish
            |> Actor.ref

        new Latch(ref)

//
//  Distributed read-only cell
//

type ImmutableCell<'T> private (source : ActorRef<IReplyChannel<'T>>) =
    member __.GetValue () = source <!- id
    /// Initialize a distributed cell from a value factory ; assume exception safe
    static member Init (f : unit -> 'T) =
        let ref =
            Actor.Stateless (fun (rc : IReplyChannel<'T>) -> rc.Reply (f ()))
            |> Actor.Publish
            |> Actor.ref

        new ImmutableCell<'T>(ref)

//
// Distributed mutable cell
//

type private CellMsg<'T> =
    | Override of 'T
    | Set of IReplyChannel<bool> * 'T
    | TryRead of IReplyChannel<'T option>

type Cell<'T> private (source: ActorRef<CellMsg<'T>>) =
    member __.Override(value: 'T) = source <-!- Override value
    member __.AsyncSet(value: 'T) = source <!- fun ch -> Set(ch, value)
    member __.Set(value: 'T) = source <!= fun ch -> Set(ch, value)
    member __.TryAsyncRead() = source <!- TryRead
    member __.TryRead() = source <!= TryRead

    static member Init(?initValue: 'T) =
        let behavior state msg =
            async {
                match msg with
                | Override value -> return (Some value)
                | Set(ch, value) when Option.isSome state ->
                    do! ch.Reply false
                    return state
                | Set(ch, value) ->
                    do! ch.Reply true
                    return (Some value)
                | TryRead ch ->
                    do! ch.Reply state
                    return state
            }

        let source =
            Actor.Stateful initValue behavior
            |> Actor.Publish
            |> Actor.ref

        new Cell<'T>(source)


//
// Store Cache Map
//
type private StoreCacheMapMsg =
    | Cache of (* worker id *) string * (* store entity ids *) string []
    | Evict of (* worker id *) string * (* store entity ids *) string []
    | GetPicture of IReplyChannel<((* store entity id *) string * (* worker ids *) string [])[]> * (* store entity ids *) string []

type StoreCacheMap private (source: ActorRef<StoreCacheMapMsg>) =
    member __.AsyncCache(workerId: string, storeEntities: string[]) = source <-!- Cache(workerId, storeEntities)
    member __.Cache(workerId: string, storeEntities: string[]) = source <-- Cache(workerId, storeEntities)
    member __.AsyncEvict(workerId: string, storeEntities: string[]) = source <-!- Evict(workerId, storeEntities)
    member __.Evict(workerId: string, storeEntities: string[]) = source <-- Evict(workerId, storeEntities)
    member __.AsyncGetPicture(storeEntities: string[]) = source <!- fun ch -> GetPicture(ch, storeEntities)
    member __.GetPicture(storeEntities: string[]) = source <!= fun ch -> GetPicture(ch, storeEntities)

    static member Init() =
        let map = new Dictionary<string, HashSet<string>>()

        let behavior msg =
            async {
                match msg with
                | Cache(workerId, storeEntities) ->
                    for storeEntity in storeEntities do
                        let isValue, value = map.TryGetValue(storeEntity)
                        if isValue then value.Add(workerId) |> ignore
                        else
                            let set = new HashSet<string>()
                            set.Add(workerId) |> ignore
                            map.Add(storeEntity, set)
                | Evict(workerId, storeEntities) ->
                    for storeEntity in storeEntities do
                        let isValue, value = map.TryGetValue(storeEntity)
                        if isValue then value.Remove(workerId) |> ignore
                | GetPicture(ch, storeEntities) ->
                    let vector = new List<_>()
                    for storeEntity in storeEntities do
                        let isValue, value = map.TryGetValue(storeEntity)
                        if isValue then let workerIds = Seq.toArray value in vector.Add(storeEntity, workerIds)

                    do! ch.Reply <| vector.ToArray()
            }

        let source =
            Actor.Stateless behavior
            |> Actor.Publish
            |> Actor.ref

        new StoreCacheMap(source)


//
//  Distributed logger
//

type Logger private (target : ActorRef<string>) =
    interface ICloudLogger with member __.Log txt = target <-- txt
    static member Init(logger : string -> unit) =
        let ref =
            Actor.Stateless (fun msg -> async { return logger msg })
            |> Actor.Publish
            |> Actor.ref

        new Logger(ref)

//
//  Distributed result aggregator
//

type private ResultAggregatorMsg<'T> =
    | SetResult of index:int * value:'T * completed:IReplyChannel<bool>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

/// A distributed resource that aggregates an array of results.
type ResultAggregator<'T> private (source : ActorRef<ResultAggregatorMsg<'T>>) =
    /// Asynchronously assign a value at given index.
    member __.SetResult(index : int, value : 'T) = source <!- fun ch -> SetResult(index, value, ch)
    /// Results the completed
    member __.ToArray () = source <!- ToArray
    /// Initializes a result aggregator of given size at the current process.
    static member Init(size : int) =
        let behaviour (results : Map<int, 'T>) msg = async {
            match msg with
            | SetResult(i, value, rc) when i < 0 || i >= size ->
                let e = new IndexOutOfRangeException()
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, rc) ->
                let results = results.Add(i, value)
                let isCompleted = results.Count = size
                do! rc.Reply isCompleted
                return results

            | IsCompleted rc ->
                do! rc.Reply ((results.Count = size))
                return results

            | ToArray rc when results.Count = size ->
                let array = results |> Map.toSeq |> Seq.sortBy fst |> Seq.map snd |> Seq.toArray
                do! rc.Reply array
                return results

            | ToArray rc ->
                let e = new InvalidOperationException("Result aggregator incomplete.")
                do! rc.ReplyWithException e
                return results
        }

        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new ResultAggregator<'T>(ref)

//
//  Distributed result cell
//

/// Result value
type Result<'T> =
    | Completed of 'T
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException
with
    member inline r.Value =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled edi -> ExceptionDispatchInfo.raiseWithCurrentStackTrace true edi

type private ResultCellMsg<'T> =
    | SetResult of Result<'T> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Result<'T> option>

/// Defines a reference to a distributed result cell instance.
type ResultCell<'T> private (source : ActorRef<ResultCellMsg<'T>>) =
    /// Try setting the result
    member c.SetResult result = source <!- fun ch -> SetResult(result, ch)
    /// Try getting the result
    member c.TryGetResult () = source <!- TryGetResult
    /// Asynchronously poll for result
    member c.AwaitResult() = async {
        let! result = source <!- TryGetResult
        match result with
        | None ->
            do! Async.Sleep 500
            return! c.AwaitResult()
        | Some r -> return r
    }

    /// Initialize a new result cell in the local process
    static member Init() : ResultCell<'T> =
        let behavior state msg = async {
            match msg with
            | SetResult (_, rc) when Option.isSome state ->
                do! rc.Reply false
                return state
            | SetResult (result, rc) ->
                do! rc.Reply true
                return (Some result)

            | TryGetResult rc ->
                do! rc.Reply state
                return state
        }

        let ref =
            Actor.Stateful None behavior
            |> Actor.Publish
            |> Actor.ref

        new ResultCell<'T>(ref)

//
//  Distributed Cancellation token sources
//

type private CancellationTokenId = string

type private CancellationTokenMsg =
    | IsCancellationRequested of IReplyChannel<bool>
    | RegisterChild of DistributedCancellationTokenSource
    | Cancel

/// Defines a distributed cancellation token source that can be cancelled
/// in the context of a distributed runtime.
and DistributedCancellationTokenSource private (source : ActorRef<CancellationTokenMsg>) =
    member __.Cancel () = source <-- Cancel
    member __.IsCancellationRequested () = source <!- IsCancellationRequested
    member private __.RegisterChild ch = source <-- RegisterChild ch
    /// Creates a System.Threading.CancellationToken that is linked
    /// to the distributed cancellation token.
    member __.GetLocalCancellationToken() =
        let cts = new System.Threading.CancellationTokenSource()

        let rec checkCancellation () = async {
            let! isCancelled = Async.Catch(source <!- IsCancellationRequested)
            match isCancelled with
            | Choice1Of2 true -> cts.Cancel()
            | Choice1Of2 false ->
                do! Async.Sleep 500
                return! checkCancellation ()
            | Choice2Of2 e ->
                do! Async.Sleep 1000
                return! checkCancellation ()
        }

        do Async.Start(checkCancellation())
        cts.Token

    /// <summary>
    ///     Initializes a new distributed cancellation token source in the current process
    /// </summary>
    /// <param name="parent">Linked parent cancellation token source</param>
    static member Init(?parent : DistributedCancellationTokenSource) =
        let behavior ((isCancelled, children) as state) msg = async {
            match msg with
            | IsCancellationRequested rc ->
                do! rc.Reply isCancelled
                return state
            | RegisterChild child when isCancelled ->
                try child.Cancel() with _ -> ()
                return state
            | RegisterChild child ->
                return (isCancelled, child :: children)
            | Cancel ->
                for ch in children do try ch.Cancel() with _ -> ()
                return (true, [])
        }

        let ref =
            Actor.Stateful (false, []) behavior
            |> Actor.Publish
            |> Actor.ref

        let dcts = new DistributedCancellationTokenSource(ref)

        match parent with
        | None -> ()
        | Some p -> p.RegisterChild dcts

        dcts

//
//  Distributed lease monitor. Tracks progress of dequeued tasks by
//  requiring heartbeats from the worker node. Triggers a fault event
//  when heartbeat threshold is exceeded. Used for the sample fault-tolerance implementation.
//

type private LeaseState =
    | Acquired
    | Released
    | Faulted

type private LeaseMonitorMsg =
    | SetLeaseState of LeaseState
    | GetLeaseState of IReplyChannel<LeaseState>

/// Distributed lease monitor instance
type LeaseMonitor private (threshold : TimeSpan, source : ActorRef<LeaseMonitorMsg>) =
    /// Declare lease to be released successfuly
    member __.Release () = source <-- SetLeaseState Released
    /// Declare fault during lease
    member __.DeclareFault () = source <-- SetLeaseState Faulted
    /// Heartbeat fault threshold
    member __.Threshold = threshold
    /// Initializes an asynchronous hearbeat sender workflow
    member __.InitHeartBeat () = async {
        let! ct = Async.CancellationToken
        let cts = CancellationTokenSource.CreateLinkedTokenSource ct
        let rec heartbeat () = async {
            try source <-- SetLeaseState Acquired with _ -> ()
            do! Async.Sleep (int threshold.TotalMilliseconds / 2)
            return! heartbeat ()
        }

        Async.Start(heartbeat(), cts.Token)
        return { new IDisposable with member __.Dispose () = cts.Cancel () }
    }

    /// <summary>
    ///     Initializes a new lease monitor.
    /// </summary>
    /// <param name="threshold">Heartbeat fault threshold.</param>
    static member Init (threshold : TimeSpan) =
        let behavior ((ls, lastRenew : DateTime) as state) msg = async {
            match msg, ls with
            | SetLeaseState _, (Faulted | Released) -> return state
            | SetLeaseState ls', Acquired -> return (ls', DateTime.Now)
            | GetLeaseState rc, Acquired when DateTime.Now - lastRenew > threshold ->
                do! rc.Reply Faulted
                return (Faulted, lastRenew)
            | GetLeaseState rc, ls ->
                do! rc.Reply ls
                return state
        }

        let actor =
            Actor.Stateful (Acquired, DateTime.Now) behavior
            |> Actor.Publish

        let faultEvent = new Event<unit> ()
        let rec poll () = async {
            let! state = actor.Ref <!- GetLeaseState
            match state with
            | Acquired ->
                do! Async.Sleep(2 * int threshold.TotalMilliseconds)
                return! poll ()
            | Released -> try actor.Stop() with _ -> ()
            | Faulted -> try faultEvent.Trigger () ; actor.Stop() with _ -> ()
        }

        Async.Start(poll ())

        faultEvent.Publish, new LeaseMonitor(threshold, actor.Ref)


//
//  Distributed, fault-tolerant queue implementation
//

type private QueueMsg<'T, 'DequeueToken> =
    | EnQueue of 'T * (* fault count *) int
    | EnQueueMultiple of 'T []
    | TryDequeue of 'DequeueToken * IReplyChannel<('T * (* fault count *) int * LeaseMonitor) option>

type private ImmutableQueue<'T> private (front : 'T list, back : 'T list) =
    static member Empty = new ImmutableQueue<'T>([],[])
    static member Singleton t = ImmutableQueue.Empty.Enqueue t
    member __.Enqueue t = new ImmutableQueue<'T>(front, t :: back)
    member __.EnqueueMultiple ts = new ImmutableQueue<'T>(front, List.rev ts @ back)
    member __.TryDequeue () =
        match front with
        | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, back))
        | [] ->
            match List.rev back with
            | [] -> None
            | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, []))

/// Provides a distributed, fault-tolerant queue implementation
type DistributedQueue<'T, 'DequeueToken'> private (source : ActorRef<QueueMsg<'T, 'DequeueToken'>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue (t, 0)
    member __.EnqueueMultiple (ts : 'T []) = source <-- EnQueueMultiple ts
    member __.TryDequeue (token) = source <!- fun ch -> TryDequeue(token, ch)

    /// Initializes a new distribued queue instance.
    static member Init(shouldDequeue : 'DequeueToken -> 'T -> bool) =
        let self = ref Unchecked.defaultof<ActorRef<QueueMsg<'T, 'DequeueToken>>>
        let behaviour (queue : ImmutableQueue<'T * int>) msg = async {
            match msg with
            | EnQueue (t, faultCount) -> return queue.Enqueue (t, faultCount)
            | EnQueueMultiple ts -> return ts |> Seq.map (fun t -> (t,0)) |> Seq.toList |> queue.EnqueueMultiple
            | TryDequeue (dt, rc) ->
                match queue.TryDequeue() with
                | Some((t, faultCount), queue') when (try shouldDequeue dt t with _ -> false) ->
                    let putBack, leaseMonitor = LeaseMonitor.Init (TimeSpan.FromSeconds 5.)
                    do! rc.Reply (Some (t, faultCount, leaseMonitor))
                    let _ = putBack.Subscribe(fun () -> self.Value <-- EnQueue (t, faultCount + 1))
                    return queue'

                | _ ->
                    do! rc.Reply None
                    return queue
        }

        self :=
            Actor.Stateful ImmutableQueue<'T * int>.Empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new DistributedQueue<'T, 'DequeueToken>(self.Value)

type private PartIndexedQueueMsg<'K, 'T> =
    | UnindexedEnqueue of 'T * (* fault count *) int
    | IndexedEnqueue of 'K * 'T * (* fault count *) int
    | TryIndexedDequeue of IReplyChannel<('T * (* fault count *) int * LeaseMonitor) option> * 'K
    | TryUnindexedDequeue of IReplyChannel<('T * (* fault count *) int * LeaseMonitor) option>

type PartIndexedQueue<'K, 'T when 'K : comparison> private (source : ActorRef<PartIndexedQueueMsg<'K, 'T>>) =
    member __.Enqueue(key: 'K, t: 'T) = source <-- IndexedEnqueue(key, t, 0)
    member __.UnindexedEnqueue(t: 'T) = source <-- UnindexedEnqueue(t, 0)
    member __.TryDequeue(key: 'K) = source <!- fun ch -> TryIndexedDequeue(ch, key)
    member __.TryUnindexedDequeue() = source <!- fun ch -> TryUnindexedDequeue(ch)

    static member Init() =
        let self = ref Unchecked.defaultof<ActorRef<PartIndexedQueueMsg<'K, 'T>>>
        let rec behaviour (queueIndex: Map<'K, ImmutableQueue<'T * int>>, unindexedQueue: ImmutableQueue<'T * int>) msg =
            async {
                match msg with
                | UnindexedEnqueue(t, faultCount) -> return queueIndex, unindexedQueue.Enqueue(t, faultCount)
                | IndexedEnqueue(k, t, faultCount) ->
                    match Map.tryFind k queueIndex with
                    | Some queue -> return queueIndex |> Map.add k (queue.Enqueue(t, faultCount)), unindexedQueue
                    | None -> return queueIndex |> Map.add k (ImmutableQueue.Singleton(t, faultCount)), unindexedQueue

                | TryIndexedDequeue(rc, k) ->
                    match Map.tryFind k queueIndex with
                    | Some queue ->
                        match queue.TryDequeue() with
                        | None -> return! behaviour (queueIndex, unindexedQueue) (TryUnindexedDequeue rc)
                        | Some((t, faultCount), queue') ->
                            let putBack, leaseMonitor = LeaseMonitor.Init (TimeSpan.FromSeconds 5.)
                            do! rc.Reply (Some (t, faultCount, leaseMonitor))
                            let _ = putBack.Subscribe(fun () -> self.Value <-- UnindexedEnqueue (t, faultCount + 1))
                            return queueIndex |> Map.add k queue', unindexedQueue
                    | None -> return! behaviour (queueIndex, unindexedQueue) (TryUnindexedDequeue rc)

                | TryUnindexedDequeue ch ->
                    match unindexedQueue.TryDequeue() with
                    | None ->
                        do! ch.Reply None
                        return queueIndex, unindexedQueue
                    | Some((t, faultCount), queue') ->
                        let putBack, leaseMonitor = LeaseMonitor.Init (TimeSpan.FromSeconds 5.)
                        do! ch.Reply (Some (t, faultCount, leaseMonitor))
                        let _ = putBack.Subscribe(fun () -> self.Value <-- UnindexedEnqueue (t, faultCount + 1))
                        return queueIndex, queue'
            }

        self :=
            Actor.Stateful (Map.empty, ImmutableQueue<'T * int>.Empty) behaviour
            |> Actor.Publish
            |> Actor.ref

        new PartIndexedQueue<'K, 'T>(self.Value)

//
//  Distributed atom implementation
//

type private tag = uint64

type private AtomMsg<'T> =
    | GetValue of IReplyChannel<tag * 'T>
    | TrySetValue of tag * 'T * IReplyChannel<bool>
    | ForceValue of 'T * IReplyChannel<unit>
    | Dispose of IReplyChannel<unit>

type Atom<'T> private (id : string, source : ActorRef<AtomMsg<'T>>) =

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = Cloud.OfAsync <| async {
            let! _,value = source <!- GetValue
            return value
        }

        member __.Dispose() = Cloud.OfAsync <| async { return! source <!- Dispose }

        member __.Force(value : 'T) = Cloud.OfAsync <| async { return! source <!- fun ch -> ForceValue(value, ch) }
        member __.Update(f : 'T -> 'T, ?maxRetries) = Cloud.OfAsync <| async {
            if maxRetries |> Option.exists (fun i -> i < 0) then
                invalidArg "maxRetries" "must be non-negative."

            let rec tryUpdate retries = async {
                let! tag, value = source <!- GetValue
                let value' = f value
                let! success = source <!- fun ch -> TrySetValue(tag, value', ch)
                if success then return ()
                else
                    match maxRetries with
                    | None -> return! tryUpdate None
                    | Some 0 -> return raise <| new OperationCanceledException("ran out of retries.")
                    | Some i -> return! tryUpdate (Some (i-1))
            }

            return! tryUpdate maxRetries
        }

    static member Init(id : string, init : 'T) =
        let behaviour (state : (uint64 * 'T) option) (msg : AtomMsg<'T>) = async {
            match state with
            | None -> // object disposed
                let e = new System.ObjectDisposedException("ActorAtom")
                match msg with
                | GetValue rc -> do! rc.ReplyWithException e
                | TrySetValue(_,_,rc) -> do! rc.ReplyWithException e
                | ForceValue(_,rc) -> do! rc.ReplyWithException e
                | Dispose rc -> do! rc.ReplyWithException e
                return state

            | Some ((tag, value) as s) ->
                match msg with
                | GetValue rc ->
                    do! rc.Reply s
                    return state
                | TrySetValue(tag', value', rc) ->
                    if tag' = tag then
                        do! rc.Reply true
                        return Some (tag + 1uL, value')
                    else
                        do! rc.Reply false
                        return state
                | ForceValue(value', rc) ->
                    do! rc.Reply ()
                    return Some (tag + 1uL, value')
                | Dispose rc ->
                    do! rc.Reply ()
                    return None
        }

        let ref =
            Actor.Stateful (Some (0uL, init)) behaviour
            |> Actor.Publish
            |> Actor.ref

        new Atom<'T>(id, ref)



//
//  Distributed channel implementation
//

type private ChannelMsg<'T> =
    | Send of 'T
    | Receive of IReplyChannel<'T>

type Channel<'T> private (id : string, source : ActorRef<ChannelMsg<'T>>) =

    interface IReceivePort<'T> with
        member __.Id = id
        member __.Receive(?timeout : int) = Cloud.OfAsync <| async { return! source.PostWithReply(Receive, ?timeout = timeout) }
        member __.Dispose () = cloud.Zero()

    interface ISendPort<'T> with
        member __.Id = id
        member __.Send(msg : 'T) =  Cloud.OfAsync <| async { return! source.AsyncPost(Send msg) }

    /// Initializes a new distributed queue instance.
    static member Init(id : string) =
        let self = ref Unchecked.defaultof<ActorRef<ChannelMsg<'T>>>
        let behaviour (messages : ImmutableQueue<'T>, receivers : ImmutableQueue<IReplyChannel<'T>>) msg = async {
            match msg with
            | Send t ->
                match receivers.TryDequeue () with
                | Some(rc, receivers') ->
                    // receiving side may have timed out a long time ago, protect
                    try
                        do! rc.Reply t
                        return (messages, receivers')
                    with e ->
                        // reply failed, re-enqueue
                        self.Value <-- Send t
                        return (messages, receivers')

                | None ->
                    return (messages.Enqueue t, receivers)

            | Receive rc ->
                match messages.TryDequeue () with
                | Some(t, senders') ->
                    do! rc.Reply t
                    return (senders', receivers)
                | None ->
                    return messages, receivers.Enqueue rc
        }

        self :=
            Actor.Stateful (ImmutableQueue.Empty, ImmutableQueue.Empty) behaviour
            |> Actor.Publish
            |> Actor.ref

        new Channel<'T>(id, self.Value)

//
//  Distributed Resource factory.
//  Provides facility for remotely deploying distributed resources.
//

type private ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

/// Provides facility for remotely deploying resources
type ResourceFactory private (source : ActorRef<ResourceFactoryMsg>) =
    member __.RequestResource<'T>(factory : unit -> 'T) = async {
        let ctor () = factory () :> obj
        let! resource = source <!- fun ch -> RequestResource(ctor, ch)
        return resource :?> 'T
    }

    member __.RequestLatch(count) = __.RequestResource(fun () -> Latch.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestCancellationTokenSource(?parent) = __.RequestResource(fun () -> DistributedCancellationTokenSource.Init(?parent = parent))
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())
    member __.RequestChannel<'T>(id) = __.RequestResource(fun () -> Channel<'T>.Init(id))
    member __.RequestAtom<'T>(id, init) = __.RequestResource(fun () -> Atom<'T>.Init(id, init))

    static member Init () =
        let behavior (RequestResource(ctor,rc)) = async {
            let r = try ctor () |> Choice1Of2 with e -> Choice2Of2 e
            match r with
            | Choice1Of2 res -> do! rc.Reply res
            | Choice2Of2 e -> do! rc.ReplyWithException e
        }

        let ref =
            Actor.Stateless behavior
            |> Actor.Publish
            |> Actor.ref

        new ResourceFactory(ref)

//
// Assembly exporter : provides assembly uploading facility for Vagabond
//

type private AssemblyExporterMsg =
    | RequestAssemblies of AssemblyId list * IReplyChannel<AssemblyPackage list>

/// Provides assembly uploading facility for Vagabond.
type AssemblyExporter private (exporter : ActorRef<AssemblyExporterMsg>) =
    static member Init() =
        let behaviour (RequestAssemblies(ids, ch)) = async {
            let packages = VagabondRegistry.Instance.CreateAssemblyPackages(ids, includeAssemblyImage = true)
            do! ch.Reply packages
        }

        let ref =
            Actor.Stateless behaviour
            |> Actor.Publish
            |> Actor.ref

        new AssemblyExporter(ref)

    /// <summary>
    ///     Request the loading of assembly dependencies from remote
    ///     assembly exporter to the local application domain.
    /// </summary>
    /// <param name="ids">Assembly id's to be loaded in app domain.</param>
    member __.LoadDependencies(ids : AssemblyId list) = async {
        let publisher =
            {
                new IRemoteAssemblyPublisher with
                    member __.GetRequiredAssemblyInfo () = async { return ids }
                    member __.PullAssemblies ids = exporter <!- fun ch -> RequestAssemblies(ids, ch)
            }

        do! VagabondRegistry.Instance.ReceiveDependencies publisher
    }

    /// <summary>
    ///     Compute assembly dependencies for provided object graph.
    /// </summary>
    /// <param name="graph">Object graph to be analyzed</param>
    member __.ComputeDependencies (graph:'T) =
        VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId


type WorkerManager =
    | SubscribeToRuntime of IReplyChannel<unit> * string * int
    | Unsubscribe of IReplyChannel<unit>

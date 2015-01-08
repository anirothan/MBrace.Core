﻿module internal Nessos.MBrace.SampleRuntime.Tasks

// Provides facility for the execution of tasks.
// In this context, a task denotes a single work item to be sent
// to a worker node for execution. Tasks may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a task.

open System
open System.Collections.Generic
open System.Threading.Tasks

open Nessos.Thespian
open Nessos.FsPickler
open Nessos.Vagrant

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Serialization
open Nessos.MBrace.Runtime.Store
open Nessos.MBrace.Runtime.Vagrant
open Nessos.MBrace.SampleRuntime.Actors

// Tasks are cloud workflows that have been attached to continuations.
// In that sense they are 'closed' multi-threaded computations that
// are difficult to reason about from a worker node's point of view.
// TaskExecutionMonitor provides a way to cooperatively track execution
// of such 'closed' computations.

/// Provides a mechanism for cooperative task execution monitoring.
[<AutoSerializable(false)>]
type TaskExecutionMonitor () =
    let tcs = TaskCompletionSource<unit> ()
    static let fromContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskExecutionMonitor> ()

    member __.Task = tcs.Task
    member __.TriggerFault (e : exn) = tcs.TrySetException e |> ignore
    member __.TriggerCompletion () = tcs.TrySetResult () |> ignore

    /// Runs a single threaded, synchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectSync ctx (f : unit -> unit) : unit =
        let tem = fromContext ctx
        try f () with e -> tem.TriggerFault e |> ignore

    /// Runs an asynchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectAsync ctx (f : Async<unit>) : unit =
        let tem = fromContext ctx
        Async.StartWithContinuations(f, ignore, tem.TriggerFault, ignore)   

    /// Triggers task completion on the contextual TaskExecutionMonitor
    static member TriggerCompletion ctx =
        let tem = fromContext ctx in tem.TriggerCompletion () |> ignore

    /// Triggers task fault on the contextual TaskExecutionMonitor
    static member TriggerFault (ctx, e) =
        let tem = fromContext ctx in tem.TriggerFault e |> ignore

    /// Asynchronously await completion of provided TaskExecutionMonitor
    static member AwaitCompletion (tem : TaskExecutionMonitor) = async {
        try
            return! Async.AwaitTask tem.Task
        with :? System.AggregateException as e when e.InnerException <> null ->
            return! Async.Raise e.InnerException
    }

/// Process information record
type ProcessInfo =
    {
        /// Cloud process unique identifier
        ProcessId : string
        /// Default file store container for process
        DefaultDirectory : string
        /// Default atom container for process
        DefaultAtomContainer : string
        /// Default channel container for process
        DefaultChannelContainer : string
    }

/// Defines a task to be executed in a worker node
type Task = 
    {
        /// Return type of the defining cloud workflow.
        Type : Type
        /// Cloud process information
        ProcessInfo : ProcessInfo
        /// Task unique identifier
        TaskId : string
        /// Triggers task execution with worker-provided execution context
        StartTask : ExecutionContext -> unit
        /// Task fault policy
        FaultPolicy : FaultPolicy
        /// Exception Continuation
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to task
        CancellationTokenSource : DistributedCancellationTokenSource
    }
with
    /// <summary>
    ///     Asynchronously executes task in the local process.
    /// </summary>
    /// <param name="runtimeProvider">Local scheduler implementation.</param>
    /// <param name="dependencies">Task dependent assemblies.</param>
    /// <param name="task">Task to be executed.</param>
    static member RunAsync (runtimeProvider : IRuntimeProvider) 
                            (channelProvider : ICloudChannelProvider) 
                            (dependencies : AssemblyId list) (faultCount : int)
                            (task : Task) = 
        async {
            let tem = new TaskExecutionMonitor()
            let ctx =
                {
                    Resources = 
                        resource { 
                            yield runtimeProvider ; yield tem ; yield task.CancellationTokenSource ; 
                            yield! Config.getStoreConfiguration task.ProcessInfo.DefaultDirectory task.ProcessInfo.DefaultAtomContainer ;
                            yield { ChannelProvider = channelProvider ; DefaultContainer = task.ProcessInfo.DefaultChannelContainer }
                            yield channelProvider ; yield dependencies 
                        }

                    CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
                }

            if faultCount > 0 then
                let faultException = new FaultException(sprintf "Fault exception when running task '%s'." task.TaskId)
                match task.FaultPolicy.Policy faultCount (faultException :> exn) with
                | None -> task.Econt ctx <| ExceptionDispatchInfo.Capture faultException
                | Some timeout ->
                    do! Async.Sleep (int timeout.TotalMilliseconds)
                    do task.StartTask ctx
            else
                do task.StartTask ctx

            return! TaskExecutionMonitor.AwaitCompletion tem
        }

// type private SchedullerMsg =
//     | ScheduleTask of task: Task * dependencies: AssemblyId list * faultCount: int * leaseMonitor: LeaseMonitor

// type private SchedullerState =
//     {
//         TaskQueue: PartIndexedQueue<string (* IWorkerRef.Id *), Pickle<Task> * AssemblyId list>
//     }

// type internal Scheduler private (source: ActorRef<SchedullerMsg>) =
//     static let behavior (state: SchedullerState) (msg: SchedullerMsg) =
//         async {
//             match msg with
//             | ScheduleTask(task, dependencies, faultCount, leaseMonitor) ->
//                 if state.WorkerQueue.Count = 0 then 
//                     return { state with UnschedulledTasks = (task, dependencies, faultCount, leaseMonitor)::state.UnschedulledTasks }
//                 else
//                     do! scheduleTask task dependencies faultCount leaseMonitor
//                     return state
//         }

//     static member Init(initWorkers: WorkerRef list) =
//         let initState = { WorkerQueue = new Queue<WorkerRef>(initWorkers); UnschedulledTasks = [] }
//         let source =
//             Actor.Stateful initState behavior
//             |> Actor.Publish
//             |> Actor.ref

//         new Scheduler(source)

//     member __.ScheduleTask(task: Task, dependencies: AssemblyId list, faultCount: int, leaseMonitor: LeaseMonitor) =
//         source <-!- ScheduleTask(task, dependencies, faultCount, leaseMonitor)



/// Defines a handle to the state of a runtime instance
/// All information pertaining to the runtime execution state
/// is contained in a single process -- the initializing client.
type RuntimeState =
    {
        /// TCP endpoint used by the runtime state container
        IPEndPoint : System.Net.IPEndPoint
        /// Reference to the global task queue employed by the runtime
        /// Queue contains pickled task and its vagrant dependency manifestppp
        TaskQueue : PartIndexedQueue<string (* IWorkerRef.Id *), Pickle<Task> * AssemblyId list>
        /// Reference to a Vagrant assembly exporting actor.
        AssemblyExporter : AssemblyExporter
        /// Reference to the runtime resource manager
        /// Used for generating latches, cancellation tokens and result cells.
        ResourceFactory : ResourceFactory
        /// returns a manifest of workers available to the cluster.
        Workers : ImmutableCell<IWorkerRef []>
        /// Track cached store entities
        StoreCacheMap : StoreCacheMap
        /// Distributed logger facility
        Logger : Logger
    }
with
    /// Initialize a new runtime state in the local process
    static member InitLocal (logger : string -> unit) (getWorkers : unit -> IWorkerRef []) =
        {
            IPEndPoint = Nessos.MBrace.SampleRuntime.Config.getLocalEndpoint()
            Workers = ImmutableCell.Init getWorkers
            StoreCacheMap = StoreCacheMap.Init()
            Logger = Logger.Init logger
            TaskQueue = PartIndexedQueue<_, _>.Init ()
            AssemblyExporter = AssemblyExporter.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    /// <summary>
    ///     Enqueue a cloud workflow with supplied continuations to the runtime task queue.
    /// </summary>
    /// <param name="dependencies">Vagrant dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="sc">Success continuation</param>
    /// <param name="ec">Exception continuation</param>
    /// <param name="cc">Cancellation continuation</param>
    /// <param name="wf">Workflow</param>
    member rt.EnqueueTask procInfo dependencies cts fp sc ec cc (wf : Cloud<'T>) =
        let taskId = System.Guid.NewGuid().ToString()
        let startTask ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)

        let task = 
            { 
                Type = typeof<'T>
                ProcessInfo = procInfo
                TaskId = taskId
                StartTask = startTask
                FaultPolicy = fp
                Econt = ec
                CancellationTokenSource = cts
            }

        let taskp = VagrantRegistry.Pickler.PickleTyped task

        let storeEntities =
            StorageEntity.GatherStoreEntitiesInObjectGraph(startTask)
            |> Seq.map (fun s -> s.Id)
            |> Seq.toArray

        if Array.length storeEntities = 0 then rt.TaskQueue.UnindexedEnqueue(taskp, dependencies)
        else
            let picture = rt.StoreCacheMap.GetPicture(storeEntities)
            let selectedWorkerId =
                picture
                |> Seq.collect (fun (storeEntity, workerIds) -> workerIds |> Seq.map (fun workerId -> storeEntity, workerId))
                |> Seq.groupBy snd
                |> Seq.sortBy (fun (workerId, data) -> -(Seq.length data))
                |> Seq.map fst
                |> Seq.head


            rt.TaskQueue.Enqueue(selectedWorkerId, (taskp, dependencies))

    /// <summary>
    ///     Schedules a cloud workflow as a distributed result cell.
    ///     Used for root-level workflows or child tasks.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to task.</param>
    /// <param name="wf">Input workflow.</param>
    member rt.StartAsCell procInfo dependencies cts fp (wf : Cloud<'T>) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let setResult ctx r = 
            async {
                let! success = resultCell.SetResult r
                TaskExecutionMonitor.TriggerCompletion ctx
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        rt.EnqueueTask procInfo dependencies cts fp scont econt ccont wf
        return resultCell
    }

    /// Attempt to dequeue a task from the runtime task queue
    member rt.TryDequeue () = async {
        let! item = rt.TaskQueue.TryUnindexedDequeue()
        match item with
        | None -> return None
        | Some ((tp, deps), faultCount, leaseMonitor) -> 
            do! rt.AssemblyExporter.LoadDependencies deps
            let task = VagrantRegistry.Pickler.UnPickleTyped tp
            return Some (task, deps, faultCount, leaseMonitor)
    }

    /// Attempt to dequeue a task from the runtime task queue of specific worker
    member rt.TryDequeue (workerId: string) = async {
        let! item = rt.TaskQueue.TryDequeue(workerId)
        match item with
        | None -> return None
        | Some ((tp, deps), faultCount, leaseMonitor) -> 
            do! rt.AssemblyExporter.LoadDependencies deps
            let task = VagrantRegistry.Pickler.UnPickleTyped tp
            return Some (task, deps, faultCount, leaseMonitor)
    }

type LocalRuntimeState =
    {
        RuntimeState: RuntimeState
        WorkerRef: IWorkerRef
    }

    static member InitLocal(workerRef: IWorkerRef, runtimeState: RuntimeState) =
        {
            RuntimeState = runtimeState
            WorkerRef = workerRef
        }

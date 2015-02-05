﻿module internal MBrace.SampleRuntime.RuntimeProvider

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.FsPickler
open Nessos.Thespian
open Nessos.Thespian.Remote

open Nessos.Vagabond

open MBrace
open MBrace.Continuation
open MBrace.Workflows
open MBrace.Runtime.InMemory
open MBrace.Runtime.Vagabond
open MBrace.Store
open MBrace.Runtime

open MBrace.SampleRuntime.Tasks
open MBrace.SampleRuntime.Actors


/// IWorkerRef implementation for the runtime
type Worker(procId : string) =
    let id = sprintf "sample runtime worker (id %s)" procId

    member __.Id = id

    interface IWorkerRef with
        member __.Id = id
        member __.Type = "sample runtime worker node"
        member __.CompareTo(other : obj) =
            match other with
            | :? Worker as w -> compare id (w :> IWorkerRef).Id
            | _ -> invalidArg "other" "invalid comparand."

    override __.ToString() = id
    override __.Equals other =
        match other with
        | :? Worker as w -> id = (w :> IWorkerRef).Id
        | _ -> false

    override __.GetHashCode() = hash id

    static member LocalWorker = new Worker(Process.GetCurrentProcess().Id.ToString())
    static member RemoteWorker(id: string) = new Worker(id)


type ActorAtomProvider (state : RuntimeState) =
    let id = state.IPEndPoint.ToString()
    interface ICloudAtomProvider with
        member x.CreateAtom(container: string, initValue: 'T): Async<ICloudAtom<'T>> = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! atom = state.ResourceFactory.RequestAtom<'T>(id, initValue)
            return atom :> ICloudAtom<'T>
        }

        member x.CreateUniqueContainerName () = System.Guid.NewGuid().ToString()

        member x.DisposeContainer (_ : string) = async.Zero()

        member x.Id: string = id

        member x.IsSupportedValue(value: 'T): bool = true

        member x.Name: string = "ActorAtom"

type ActorChannelProvider (state : RuntimeState) =
    let id = state.IPEndPoint.ToString()
    interface ICloudChannelProvider with
        member __.Name = "ActorChannel"
        member __.Id = id
        member __.CreateUniqueContainerName () = ""

        member __.CreateChannel<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! ch = state.ResourceFactory.RequestChannel<'T> id
            return ch :> ISendPort<'T>, ch :> IReceivePort<'T>
        }

        member __.DisposeContainer _ = async.Zero()

/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, procInfo : ProcessInfo, dependencies : AssemblyId [], faultPolicy, taskId, context) =

    let failTargetWorker () = invalidOp <| sprintf "Cannot target worker when running in '%A' execution context" context

    let extractComputations (computations : seq<Cloud<_> * IWorkerRef option>) =
        computations
        |> Seq.map (fun (c,w) -> if Option.isSome w then failTargetWorker () else c)
        |> Seq.toArray

    /// Creates a runtime provider instance for a provided task
    static member FromTask state dependencies (task : Task) =
        new RuntimeProvider(state, task.ProcessInfo, dependencies, task.FaultPolicy, task.TaskId, Distributed)

    interface ICloudRuntimeProvider with
        member __.ProcessId = procInfo.ProcessId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext ctx =
            match ctx, context with
            | Distributed, (ThreadParallel | Sequential)
            | ThreadParallel, Sequential ->
                invalidOp <| sprintf "Cannot set scheduling context to '%A' when it already is '%A'." ctx context
            | _ ->
                new RuntimeProvider(state, procInfo, dependencies, faultPolicy, taskId, ctx) :> ICloudRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy =
            new RuntimeProvider(state, procInfo, dependencies, newPolicy, taskId, context) :> ICloudRuntimeProvider

        member __.IsTargetedWorkerSupported =
            match context with
            | Distributed -> true
            | _ -> false

        member __.ScheduleParallel computations =
            match context with
            | Distributed -> Combinators.Parallel state procInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Parallel (extractComputations computations)
            | Sequential -> Sequential.Parallel (extractComputations computations)

        member __.ScheduleChoice computations =
            match context with
            | Distributed -> Combinators.Choice state procInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Choice (extractComputations computations)
            | Sequential -> Sequential.Choice (extractComputations computations)

        member __.ScheduleStartChild(computation,worker,_) =
            match context with
            | Distributed -> Combinators.StartChild state procInfo dependencies faultPolicy worker computation
            | _ when Option.isSome worker -> failTargetWorker ()
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = state.Workers.GetValue()
        member __.CurrentWorker = Worker.LocalWorker :> IWorkerRef
        member __.Logger = state.Logger :> ICloudLogger

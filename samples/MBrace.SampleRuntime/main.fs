﻿module internal MBrace.SampleRuntime.Main

    open Nessos.Thespian
    open Nessos.Thespian.Remote.Protocols
    open MBrace
    open MBrace.Continuation
    open MBrace.Runtime
    open MBrace.SampleRuntime.Actors
    open MBrace.SampleRuntime.RuntimeProvider
    open MBrace.SampleRuntime.Tasks
    open MBrace.SampleRuntime.Worker

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            do Config.Init()
            let logger = new ConsoleTaskLogger()
            let evaluator =
#if APPDOMAIN_ISOLATION
                new AppDomainTaskEvaluator() :> ITaskEvaluator
#else
                new LocalTaskEvaluator() :> ITaskEvaluator
#endif
            if args.Length > 0 then
                let runtime = Argument.toRuntime args
                let workerRef = new Worker(System.Diagnostics.Process.GetCurrentProcess().Id.ToString()) :> IWorkerRef
                Async.RunSync (Worker.initWorker runtime logger maxConcurrentTasks evaluator)
                0
            else
                Actor.Stateful (new System.Threading.CancellationTokenSource()) (Worker.workerManager logger evaluator)
                |> Actor.rename "workerManager"
                |> Actor.publish [ Protocols.utcp() ]
                |> Actor.start
                |> ignore
                Async.RunSynchronously <| async { while true do do! Async.Sleep 10000 }
                0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1

﻿namespace MBrace.Core.Tests

open FsCheck

open System.Collections.Generic
open System.IO
open System.Threading

open NUnit.Framework

open MBrace.Store.Internals

[<AutoOpen>]
module Utils =

    let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
    let isAppVeyorInstance = System.Environment.GetEnvironmentVariable("APPVEYOR") <> null
    let isTravisInstance = System.Environment.GetEnvironmentVariable("TRAVIS") <> null

    let shouldfail (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "expected exception but was value '%A'" v)
        with _ -> ()

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "expected exception but was value '%A'" v)
        with :? 'Exn -> ()

    /// type safe equality tester
    let shouldEqual (expected : 'T) (input : 'T) = 
        if expected = input then ()
        else
            raise <| new AssertionException(sprintf "expected '%A' but was '%A'." expected input)

    let shouldBe (pred : 'T -> bool) (input : 'T) =
        if pred input then ()
        else
            raise <| new AssertionException(sprintf "value '%A' does not match predicate." input)

    type ISerializer with
        member s.Clone<'T>(t : 'T) =
            use m = new MemoryStream()
            s.Serialize(m, t, leaveOpen = true)
            m.Position <- 0L
            s.Deserialize<'T>(m, leaveOpen = true)

    [<RequireQualifiedAccess>]
    module Choice =

        let protect (f : unit -> 'T) =
            try f () |> Choice1Of2 with e -> Choice2Of2 e

        let shouldEqual (value : 'T) (input : Choice<'T, exn>) = 
            match input with
            | Choice1Of2 v' -> shouldEqual value v'
            | Choice2Of2 e -> raise e

        let shouldBe (pred : 'T -> bool) (input : Choice<'T, exn>) =
            match input with
            | Choice1Of2 t when pred t -> ()
            | Choice1Of2 t -> raise <| new AssertionException(sprintf "value '%A' does not match predicate." t)
            | Choice2Of2 e -> raise e

        let shouldFailwith<'T, 'Exn when 'Exn :> exn> (input : Choice<'T, exn>) = 
            match input with
            | Choice1Of2 t -> raise <| new AssertionException(sprintf "Expected exception, but was value '%A'." t)
            | Choice2Of2 (:? 'Exn) -> ()
            | Choice2Of2 e -> raise e

    /// repeats computation (test) for a given number of times
    let repeat (maxRepeats : int) (f : unit -> unit) : unit =
        for _ in 1 .. maxRepeats do f ()

    type Check =
        /// quick check methods with explicit type annotation
        static member QuickThrowOnFail<'T> (f : 'T -> unit, ?maxRuns) = 
            match maxRuns with
            | None -> Check.QuickThrowOnFailure f
            | Some mxrs -> Check.One({ Config.QuickThrowOnFailure with MaxTest = mxrs }, f)

        /// quick check methods with explicit type annotation
        static member QuickThrowOnFail<'T> (f : 'T -> bool, ?maxRuns) = 
            match maxRuns with
            | None -> Check.QuickThrowOnFailure f
            | Some mxrs -> Check.One({ Config.QuickThrowOnFailure with MaxTest = mxrs }, f)

    [<AutoSerializable(false)>]
    type private DisposableEnumerable<'T>(isDisposed : bool ref, ts : seq<'T>) =
        let check() = if !isDisposed then raise <| new System.ObjectDisposedException("enumerator")
        let e = ts.GetEnumerator()
        interface IEnumerator<'T> with
            member __.Current = check () ; e.Current
            member __.Current = check () ; box e.Current
            member __.MoveNext () = check () ; e.MoveNext()
            member __.Dispose () = check () ; isDisposed := true ; e.Dispose()
            member __.Reset () = check () ; e.Reset()
            
    [<AutoSerializable(true)>]
    type DisposableSeq<'T> (ts : seq<'T>) =
        let isDisposed = ref false

        member __.IsDisposed = !isDisposed

        interface seq<'T> with
            member __.GetEnumerator() = new DisposableEnumerable<'T>(isDisposed, ts) :> IEnumerator<'T>
            member __.GetEnumerator() = new DisposableEnumerable<'T>(isDisposed, ts) :> System.Collections.IEnumerator

    let dseq ts = new DisposableSeq<'T>(ts)
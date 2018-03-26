package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.StrictCurrentTraceContext;
import brave.propagation.TraceContext;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.MaybeObserver;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.functions.Predicate;
import io.reactivex.internal.fuseable.ScalarCallable;
import io.reactivex.internal.operators.completable.CompletableFromCallable;
import io.reactivex.internal.operators.flowable.FlowableFromCallable;
import io.reactivex.internal.operators.maybe.MaybeFromCallable;
import io.reactivex.internal.operators.observable.ObservableFromCallable;
import io.reactivex.internal.operators.single.SingleFromCallable;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.observers.TestObserver;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.subscribers.TestSubscriber;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class CurrentTraceContextAssemblyTrackingTest {
  StrictCurrentTraceContext currentTraceContext = new StrictCurrentTraceContext();
  CurrentTraceContextAssemblyTracking contextPlugins = CurrentTraceContextAssemblyTracking.create(
      currentTraceContext
  );
  TraceContext context1 = TraceContext.newBuilder().traceId(1L).spanId(1L).build();
  TraceContext context2 = context1.toBuilder().parentId(1L).spanId(2L).build();
  Predicate<Integer> lessThanThree = i -> {
    assertContext1();
    return i < 3;
  };

  @Before @After public void setup() {
    RxJavaPlugins.reset();
  }

  @Test public void completable() {
    contextPlugins.enable();

    Completable source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Completable.complete()
          .doOnComplete(() -> assertContext1());
      errorSource = Completable.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult();
  }

  @Test public void flowable() {
    contextPlugins.enable();

    Flowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Flowable.range(1, 3)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1());
      errorSource = Flowable.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1, 2, 3);
  }

  @Test public void flowable_conditional() {
    contextPlugins.enable();

    Flowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Flowable.range(1, 3)
          .filter(lessThanThree)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1());
      errorSource = Flowable.<Integer>error(new IllegalStateException())
          .filter(lessThanThree)
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1, 2);
  }

  @Test public void observable() {
    contextPlugins.enable();

    Observable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Observable.range(1, 3)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1());
      errorSource = Observable.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source, errorSource)
        .assertResult(1, 2, 3);
  }

  @Test public void single() {
    contextPlugins.enable();

    Single<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Single.just(1)
          .doOnSuccess(i -> assertContext1());
      errorSource = Single.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
  }

  @Test public void maybe() {
    contextPlugins.enable();

    Maybe<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Maybe.just(1)
          .doOnSuccess(i -> assertContext1());
      errorSource = Maybe.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
  }

  @Test public void parallelFlowable() {
    contextPlugins.enable();

    ParallelFlowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Flowable.range(1, 3).parallel()
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1());
      errorSource = Flowable.concat(
          Flowable.<Integer>error(new IllegalStateException()),
          Flowable.<Integer>error(new IllegalStateException())
      ).parallel()
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source, errorSource).assertResult(1, 2, 3);
  }

  @Test public void parallelFlowable_conditional() {
    contextPlugins.enable();

    ParallelFlowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Flowable.range(1, 3).parallel()
          .filter(lessThanThree)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1());
      errorSource = Flowable.concat(
          Flowable.<Integer>error(new IllegalStateException()),
          Flowable.<Integer>error(new IllegalStateException())
      ).parallel()
          .filter(lessThanThree)
          .doOnError(t -> assertContext1());
    }

    subscribeUnderScope2(source, errorSource).assertResult(1, 2);
  }

  @Test public void flowable_connectable() {
    contextPlugins.enable();

    ConnectableFlowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Flowable.range(1, 3)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1()).publish();
      errorSource = Flowable.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1()).publish();
    }

    subscribeUnderScope2(
        source.autoConnect().toObservable(),
        errorSource.autoConnect().toObservable()
    ).assertResult(1, 2, 3);
  }

  @Test public void flowable_connectable_conditional() {
    contextPlugins.enable();

    ConnectableFlowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Flowable.range(1, 3)
          .filter(lessThanThree)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1()).publish();
      errorSource = Flowable.<Integer>error(new IllegalStateException())
          .filter(lessThanThree)
          .doOnError(t -> assertContext1()).publish();
    }

    subscribeUnderScope2(
        source.autoConnect().toObservable(),
        errorSource.autoConnect().toObservable()
    ).assertResult(1, 2);
  }

  @Test public void observable_connectable() {
    contextPlugins.enable();

    ConnectableObservable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = Observable.range(1, 3)
          .doOnNext(i -> assertContext1())
          .doOnComplete(() -> assertContext1()).publish();
      errorSource = Observable.<Integer>error(new IllegalStateException())
          .doOnError(t -> assertContext1()).publish();
    }

    subscribeUnderScope2(source.autoConnect(), errorSource.autoConnect())
        .assertResult(1, 2, 3);
  }

  @Test public void callable_completable() throws Exception {
    contextPlugins.enable();

    Completable source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new CallableCompletable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new CallableCompletable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertComplete();
    callUnderScope2((Callable<Integer>) source, (Callable<Integer>) errorSource);
  }

  @Test public void callable_flowable() throws Exception {
    contextPlugins.enable();

    Flowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new CallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new CallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void callable_flowable_conditional() throws Exception {
    contextPlugins.enable();

    Flowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new CallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      }).filter(i -> i < 1);
      errorSource = RxJavaPlugins.onAssembly(new CallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      }).filter(i -> i < 1);
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult();
  }

  @Test public void callable_maybe() throws Exception {
    contextPlugins.enable();

    Maybe<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new CallableMaybe<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new CallableMaybe<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void callable_observable() throws Exception {
    contextPlugins.enable();

    Observable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new CallableObservable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new CallableObservable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source, errorSource).assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void callable_single() throws Exception {
    contextPlugins.enable();

    Single<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new CallableSingle<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new CallableSingle<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void scalarcallable_completable() throws Exception {
    contextPlugins.enable();

    Completable source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new ScalarCallableCompletable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new ScalarCallableCompletable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult();
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void scalarcallable_flowable() throws Exception {
    contextPlugins.enable();

    Flowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new ScalarCallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new ScalarCallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void scalarcallable_flowable_conditional() throws Exception {
    contextPlugins.enable();

    Flowable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new ScalarCallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      }).filter(i -> i < 1);
      errorSource = RxJavaPlugins.onAssembly(new ScalarCallableFlowable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      }).filter(i -> i < 1);
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult();
  }

  @Test public void scalarcallable_maybe() throws Exception {
    contextPlugins.enable();

    Maybe<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new ScalarCallableMaybe<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new ScalarCallableMaybe<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void scalarcallable_observable() throws Exception {
    contextPlugins.enable();

    Observable<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new ScalarCallableObservable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new ScalarCallableObservable<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source, errorSource)
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  @Test public void scalarcallable_single() throws Exception {
    contextPlugins.enable();

    Single<Integer> source, errorSource;
    try (Scope scope = currentTraceContext.newScope(context1)) {
      source = RxJavaPlugins.onAssembly(new ScalarCallableSingle<Integer>() {
        @Override public Integer call() {
          assertContext1();
          return 1;
        }
      });
      errorSource = RxJavaPlugins.onAssembly(new ScalarCallableSingle<Integer>() {
        @Override public Integer call() {
          assertContext1();
          throw new IllegalStateException();
        }
      });
    }

    subscribeUnderScope2(source.toObservable(), errorSource.toObservable())
        .assertResult(1);
    callUnderScope2((Callable) source, (Callable) errorSource);
  }

  TestObserver<Integer> subscribeUnderScope2(
      Observable<Integer> source,
      Observable<Integer> errorSource
  ) {
    // Set another span between the time the source was created and subscribed.
    try (Scope scope2 = currentTraceContext.newScope(context2)) {
      // callbacks after test subscribes should be in the second context
      errorSource
          .doOnSubscribe(s -> assertContext1())
          .doOnError(r -> assertContext2())
          .test().assertFailure(IllegalStateException.class);

      return source
          .doOnSubscribe(s -> assertContext1())
          .doOnComplete(() -> assertContext2())
          .test();
    }
  }

  TestSubscriber<Integer> subscribeUnderScope2(
      ParallelFlowable<Integer> source,
      ParallelFlowable<Integer> errorSource
  ) {
    // Set another span between the time the source was created and subscribed.
    try (Scope scope2 = currentTraceContext.newScope(context2)) {
      // callbacks after test subscribes should be in the second context
      errorSource
          .doOnSubscribe(s -> assertContext1())
          .doOnError(r -> assertContext2())
          .sequential().test().assertFailure(IllegalStateException.class);

      return source
          .doOnSubscribe(s -> assertContext1())
          .doOnComplete(() -> assertContext2())
          .sequential().test();
    }
  }

  void assertContext1() {
    assertThat(currentTraceContext.get())
        .isEqualTo(context1);
  }

  void assertContext2() {
    assertThat(currentTraceContext.get())
        .isEqualTo(context2);
  }

  <T> void callUnderScope2(Callable<T> source, Callable<T> errorSource) throws Exception {
    // Set another span between the time the callable was created and invoked.
    try (Scope scope2 = currentTraceContext.newScope(context2)) {
      try {
        errorSource.call();
        failBecauseExceptionWasNotThrown(IllegalStateException.class);
      } catch (IllegalStateException e) {
      }

      source.call();
    }
  }

  abstract class CallableCompletable<T> extends Completable implements Callable<T> {
    final CompletableFromCallable delegate = new CompletableFromCallable(this);

    @Override protected void subscribeActual(CompletableObserver s) {
      delegate.subscribe(s);
    }
  }

  abstract class CallableFlowable<T> extends Flowable<T> implements Callable<T> {
    final FlowableFromCallable<T> delegate = new FlowableFromCallable<>(this);

    @Override protected void subscribeActual(Subscriber<? super T> s) {
      delegate.subscribe(s);
    }
  }

  abstract class CallableMaybe<T> extends Maybe<T> implements Callable<T> {
    final MaybeFromCallable<T> delegate = new MaybeFromCallable<>(this);

    @Override protected void subscribeActual(MaybeObserver<? super T> s) {
      assertContext1();
      delegate.subscribe(s);
    }
  }

  abstract class CallableObservable<T> extends Observable<T> implements Callable<T> {
    final ObservableFromCallable<T> delegate = new ObservableFromCallable<>(this);

    @Override protected void subscribeActual(Observer<? super T> s) {
      assertContext1();
      delegate.subscribe(s);
    }
  }

  abstract class CallableSingle<T> extends Single<T> implements Callable<T> {
    final SingleFromCallable<T> delegate = new SingleFromCallable<>(this);

    @Override protected void subscribeActual(SingleObserver<? super T> s) {
      assertContext1();
      delegate.subscribe(s);
    }
  }

  abstract class ScalarCallableCompletable<T> extends Completable implements ScalarCallable<T> {
    final CompletableFromCallable delegate = new CompletableFromCallable(this);

    @Override protected void subscribeActual(CompletableObserver s) {
      delegate.subscribe(s);
    }
  }

  abstract class ScalarCallableFlowable<T> extends Flowable<T> implements ScalarCallable<T> {
    final FlowableFromCallable<T> delegate = new FlowableFromCallable<>(this);

    @Override protected void subscribeActual(Subscriber<? super T> s) {
      delegate.subscribe(s);
    }
  }

  abstract class ScalarCallableMaybe<T> extends Maybe<T> implements ScalarCallable<T> {
    final MaybeFromCallable<T> delegate = new MaybeFromCallable<>(this);

    @Override protected void subscribeActual(MaybeObserver<? super T> s) {
      assertContext1();
      delegate.subscribe(s);
    }
  }

  abstract class ScalarCallableObservable<T> extends Observable<T> implements ScalarCallable<T> {
    final ObservableFromCallable<T> delegate = new ObservableFromCallable<>(this);

    @Override protected void subscribeActual(Observer<? super T> s) {
      assertContext1();
      delegate.subscribe(s);
    }
  }

  abstract class ScalarCallableSingle<T> extends Single<T> implements ScalarCallable<T> {
    final SingleFromCallable<T> delegate = new SingleFromCallable<>(this);

    @Override protected void subscribeActual(SingleObserver<? super T> s) {
      assertContext1();
      delegate.subscribe(s);
    }
  }
}

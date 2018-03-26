package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.ScalarCallable;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Prevents traces from breaking during RxJava operations by scoping them with trace context.
 *
 * <p>The design of this library borrows heavily from https://github.com/akaita/RxJava2Debug
 */
public final class CurrentTraceContextAssemblyTracking {
  /** Prevents overlapping plugin configuration */
  static final AtomicBoolean lock = new AtomicBoolean();

  final CurrentTraceContext currentTraceContext;

  CurrentTraceContextAssemblyTracking(CurrentTraceContext currentTraceContext) {
    if (currentTraceContext == null) throw new NullPointerException("currentTraceContext == null");
    this.currentTraceContext = currentTraceContext;
  }

  public static CurrentTraceContextAssemblyTracking create(CurrentTraceContext delegate) {
    return new CurrentTraceContextAssemblyTracking(delegate);
  }

  public void enable() {
    if (!lock.compareAndSet(false, true)) return;

    RxJavaPlugins.setOnObservableAssembly(new Function<Observable, Observable>() {
      @Override public Observable apply(Observable o) {
        if (!(o instanceof Callable)) {
          return new TraceContextObservable(o, currentTraceContext);
        }
        if (o instanceof ScalarCallable) {
          return new TraceContextScalarCallableObservable(o, currentTraceContext);
        }
        return new TraceContextCallableObservable(o, currentTraceContext);
      }
    });

    RxJavaPlugins.setOnConnectableObservableAssembly(
        new Function<ConnectableObservable, ConnectableObservable>() {
          @Override public ConnectableObservable apply(ConnectableObservable co) {
            return new TraceContextConnectableObservable(co, currentTraceContext);
          }
        });

    RxJavaPlugins.setOnCompletableAssembly(new Function<Completable, Completable>() {
      @Override public Completable apply(Completable c) {
        if (!(c instanceof Callable)) {
          return new TraceContextCompletable(c, currentTraceContext);
        }
        if (c instanceof ScalarCallable) {
          return new TraceContextScalarCallableCompletable(c, currentTraceContext);
        }
        return new TraceContextCallableCompletable(c, currentTraceContext);
      }
    });

    RxJavaPlugins.setOnSingleAssembly(new Function<Single, Single>() {
      @Override public Single apply(Single s) {
        if (!(s instanceof Callable)) {
          return new TraceContextSingle(s, currentTraceContext);
        }
        if (s instanceof ScalarCallable) {
          return new TraceContextScalarCallableSingle(s, currentTraceContext);
        }
        return new TraceContextCallableSingle(s, currentTraceContext);
      }
    });

    RxJavaPlugins.setOnMaybeAssembly(new Function<Maybe, Maybe>() {
      @Override public Maybe apply(Maybe m) {
        if (!(m instanceof Callable)) {
          return new TraceContextMaybe(m, currentTraceContext);
        }
        if (m instanceof ScalarCallable) {
          return new TraceContextScalarCallableMaybe(m, currentTraceContext);
        }
        return new TraceContextCallableMaybe(m, currentTraceContext);
      }
    });

    RxJavaPlugins.setOnFlowableAssembly(new Function<Flowable, Flowable>() {
      @Override public Flowable apply(Flowable f) {
        if (!(f instanceof Callable)) {
          return new TraceContextFlowable(f, currentTraceContext);
        }
        if (f instanceof ScalarCallable) {
          return new TraceContextScalarCallableFlowable(f, currentTraceContext);
        }
        return new TraceContextCallableFlowable(f, currentTraceContext);
      }
    });

    RxJavaPlugins.setOnConnectableFlowableAssembly(
        new Function<ConnectableFlowable, ConnectableFlowable>() {
          @Override public ConnectableFlowable apply(ConnectableFlowable cf) {
            return new TraceContextConnectableFlowable(cf, currentTraceContext);
          }
        }
    );

    RxJavaPlugins.setOnParallelAssembly(new Function<ParallelFlowable, ParallelFlowable>() {
      @Override
      public ParallelFlowable apply(ParallelFlowable pf) {
        return new TraceContextParallelFlowable(pf, currentTraceContext);
      }
    });

    lock.set(false);
  }

  public static void disable() {
    if (!lock.compareAndSet(false, true)) return;

    RxJavaPlugins.setOnObservableAssembly(null);
    RxJavaPlugins.setOnConnectableObservableAssembly(null);
    RxJavaPlugins.setOnCompletableAssembly(null);
    RxJavaPlugins.setOnSingleAssembly(null);
    RxJavaPlugins.setOnMaybeAssembly(null);
    RxJavaPlugins.setOnFlowableAssembly(null);
    RxJavaPlugins.setOnConnectableFlowableAssembly(null);
    RxJavaPlugins.setOnParallelAssembly(null);

    lock.set(false);
  }
}

package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.internal.fuseable.QueueDisposable;
import io.reactivex.internal.observers.BasicFuseableObserver;

final class TraceContextObservable<T> extends Observable<T> implements TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final ObservableSource<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextObservable(ObservableSource<T> source, CurrentTraceContext currentTraceContext) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(io.reactivex.Observer<? super T> s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.subscribe(new Observer<>(s, currentTraceContext, invocationContext));
    }
  }

  static final class Observer<T> extends BasicFuseableObserver<T, T> {
    final CurrentTraceContext currentTraceContext;
    final TraceContext invocationContext;

    Observer(
        io.reactivex.Observer<T> actual,
        CurrentTraceContext currentTraceContext,
        TraceContext invocationContext
    ) {
      super(actual);
      this.currentTraceContext = currentTraceContext;
      this.invocationContext = invocationContext;
    }

    @Override public void onNext(T t) {
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onNext(t);
      }
    }

    @Override public void onError(Throwable t) {
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onError(t);
      }
    }

    @Override public void onComplete() {
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onComplete();
      }
    }

    @Override public int requestFusion(int mode) {
      QueueDisposable<T> qs = this.qs;
      if (qs != null) {
        int m = qs.requestFusion(mode);
        sourceMode = m;
        return m;
      }
      return NONE;
    }

    @Override public T poll() throws Exception {
      return qs.poll();
    }
  }
}

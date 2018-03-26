package brave.context.rxjava2;

import brave.context.rxjava2.TraceContextObservable.Observer;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.internal.fuseable.ScalarCallable;

final class TraceContextScalarCallableObservable<T> extends Observable<T>
    implements ScalarCallable<T>, TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final ObservableSource<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextScalarCallableObservable(
      ObservableSource<T> source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(io.reactivex.Observer<? super T> s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.subscribe(new Observer<>(s, currentTraceContext, invocationContext));
    }
  }

  @SuppressWarnings("unchecked") @Override public T call() {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      return ((ScalarCallable<T>) source).call();
    }
  }
}

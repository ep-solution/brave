package brave.context.rxjava2;

import brave.context.rxjava2.TraceContextObservable.Observer;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import java.util.concurrent.Callable;

final class TraceContextCallableObservable<T> extends Observable<T>
    implements Callable<T>, TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final ObservableSource<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextCallableObservable(
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

  @SuppressWarnings("unchecked") @Override public T call() throws Exception {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      return ((Callable<T>) source).call();
    }
  }
}

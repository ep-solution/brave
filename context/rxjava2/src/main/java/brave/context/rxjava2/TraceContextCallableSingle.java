package brave.context.rxjava2;

import brave.context.rxjava2.TraceContextSingle.Observer;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.SingleSource;
import java.util.concurrent.Callable;

final class TraceContextCallableSingle<T> extends Single<T>
    implements Callable<T>, TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final SingleSource<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextCallableSingle(
      SingleSource<T> source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(SingleObserver<? super T> s) {
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

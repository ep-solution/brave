package brave.context.rxjava2;

import brave.context.rxjava2.TraceContextCompletable.Observer;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.internal.fuseable.ScalarCallable;

final class TraceContextScalarCallableCompletable<T> extends Completable
    implements ScalarCallable<T>, TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final CompletableSource source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextScalarCallableCompletable(
      CompletableSource source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(CompletableObserver s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.subscribe(new Observer(s, currentTraceContext, invocationContext));
    }
  }

  @SuppressWarnings("unchecked") @Override public T call() {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      return ((ScalarCallable<T>) source).call();
    }
  }
}

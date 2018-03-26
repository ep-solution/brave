package brave.context.rxjava2;

import brave.context.rxjava2.TraceContextMaybe.Observer;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Maybe;
import io.reactivex.MaybeObserver;
import io.reactivex.MaybeSource;
import io.reactivex.internal.fuseable.ScalarCallable;

final class TraceContextScalarCallableMaybe<T> extends Maybe<T>
    implements ScalarCallable<T>, TraceContextGetter {
  final MaybeSource<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextScalarCallableMaybe(
      MaybeSource<T> source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  @Override protected void subscribeActual(MaybeObserver<? super T> s) {
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

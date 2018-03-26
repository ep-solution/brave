package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.ConditionalSubscriber;
import io.reactivex.internal.fuseable.ScalarCallable;
import org.reactivestreams.Publisher;

final class TraceContextScalarCallableFlowable<T> extends Flowable<T>
    implements ScalarCallable<T>, TraceContextGetter {
  final Publisher<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextScalarCallableFlowable(
      Publisher<T> source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  @Override protected void subscribeActual(org.reactivestreams.Subscriber<? super T> s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      if (s instanceof ConditionalSubscriber) {
        source.subscribe(new TraceContextConditionalSubscriber<>(
            (ConditionalSubscriber) s, currentTraceContext, invocationContext
        ));
      } else {
        source.subscribe(new TraceContextSubscriber<>(s, currentTraceContext, invocationContext));
      }
    }
  }

  @SuppressWarnings("unchecked") @Override public T call() {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      return ((ScalarCallable<T>) source).call();
    }
  }
}

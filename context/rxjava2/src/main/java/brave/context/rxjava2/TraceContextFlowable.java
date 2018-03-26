package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.ConditionalSubscriber;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

final class TraceContextFlowable<T> extends Flowable<T> implements TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final Publisher<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextFlowable(Publisher<T> source, CurrentTraceContext currentTraceContext) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(Subscriber s) {
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
}

package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.disposables.Disposable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.fuseable.ConditionalSubscriber;

final class TraceContextConnectableFlowable<T> extends ConnectableFlowable<T> {
  final ConnectableFlowable<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextConnectableFlowable(
      ConnectableFlowable<T> source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
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

  @Override public void connect(Consumer<? super Disposable> connection) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.connect(connection);
    }
  }
}

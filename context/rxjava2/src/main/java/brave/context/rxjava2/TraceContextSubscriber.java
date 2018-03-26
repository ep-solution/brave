package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.internal.fuseable.QueueSubscription;
import io.reactivex.internal.subscribers.BasicFuseableSubscriber;

final class TraceContextSubscriber<T> extends BasicFuseableSubscriber<T, T> {

  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextSubscriber(
      org.reactivestreams.Subscriber actual,
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
    QueueSubscription<T> qs = this.qs;
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
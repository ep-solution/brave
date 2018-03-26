package brave.context.rxjava2;

import brave.context.rxjava2.TraceContextObservable.Observer;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.observables.ConnectableObservable;

final class TraceContextConnectableObservable<T> extends ConnectableObservable<T> {

  final ConnectableObservable<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextConnectableObservable(
      ConnectableObservable<T> source,
      CurrentTraceContext currentTraceContext
  ) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(io.reactivex.Observer s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.subscribe(new Observer<T>(s, currentTraceContext, invocationContext));
    }
  }

  @Override public void connect(Consumer<? super Disposable> connection) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.connect(connection);
    }
  }
}

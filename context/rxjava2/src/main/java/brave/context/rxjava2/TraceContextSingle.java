package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.SingleSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;

final class TraceContextSingle<T> extends Single<T> implements TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final SingleSource<T> source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextSingle(SingleSource<T> source, CurrentTraceContext currentTraceContext) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(SingleObserver<? super T> s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.subscribe(new Observer<>(s, currentTraceContext, invocationContext));
    }
  }

  static final class Observer<T> implements SingleObserver<T>, Disposable {
    final SingleObserver<T> actual;
    final CurrentTraceContext currentTraceContext;
    final TraceContext invocationContext;
    Disposable d;

    Observer(
        SingleObserver actual,
        CurrentTraceContext currentTraceContext,
        TraceContext invocationContext
    ) {
      this.actual = actual;
      this.currentTraceContext = currentTraceContext;
      this.invocationContext = invocationContext;
    }

    @Override public void onSubscribe(Disposable d) {
      if (!DisposableHelper.validate(this.d, d)) return;
      this.d = d;
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onSubscribe(this);
      }
    }

    @Override public void onError(Throwable t) {
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onError(t);
      }
    }

    @Override public void onSuccess(T value) {
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onSuccess(value);
      }
    }

    @Override public boolean isDisposed() {
      return d.isDisposed();
    }

    @Override public void dispose() {
      d.dispose();
    }
  }
}

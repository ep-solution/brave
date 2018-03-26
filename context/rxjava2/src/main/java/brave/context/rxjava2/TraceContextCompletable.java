package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;

final class TraceContextCompletable extends Completable implements TraceContextGetter {
  @Override public TraceContext traceContext() {
    return invocationContext;
  }

  final CompletableSource source;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextCompletable(CompletableSource source, CurrentTraceContext currentTraceContext) {
    this.source = source;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = currentTraceContext.get();
  }

  @Override protected void subscribeActual(CompletableObserver s) {
    try (Scope scope = currentTraceContext.newScope(invocationContext)) {
      source.subscribe(new Observer(s, currentTraceContext, invocationContext));
    }
  }

  static final class Observer implements CompletableObserver, Disposable {
    final CompletableObserver actual;
    final CurrentTraceContext currentTraceContext;
    final TraceContext invocationContext;
    Disposable d;

    Observer(
        CompletableObserver actual,
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

    @Override public void onComplete() {
      try (Scope scope = currentTraceContext.newScope(invocationContext)) {
        actual.onComplete();
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

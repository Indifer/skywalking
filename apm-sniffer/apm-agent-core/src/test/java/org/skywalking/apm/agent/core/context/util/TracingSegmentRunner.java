package org.skywalking.apm.agent.core.context.util;

import java.lang.reflect.Field;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.skywalking.apm.agent.core.context.IgnoreTracerContextListener;
import org.skywalking.apm.agent.core.context.IgnoredTracerContext;
import org.skywalking.apm.agent.core.context.TracingContext;
import org.skywalking.apm.agent.core.context.TracingContextListener;
import org.skywalking.apm.agent.core.context.trace.TraceSegment;

public class TracingSegmentRunner extends BlockJUnit4ClassRunner {
    private TracingContextListener tracingContextListener;
    private IgnoreTracerContextListener ignoreTracerContextListener;
    private Field field;
    private Object targetObject;
    private SegmentStorage tracingData;

    public TracingSegmentRunner(Class<?> klass) throws InitializationError {
        super(klass);
        for (Field field : klass.getDeclaredFields()) {
            if (field.isAnnotationPresent(SegmentStoragePoint.class) && field.getType().equals(SegmentStorage.class)) {
                this.field = field;
                this.field.setAccessible(true);
                break;
            }
        }
    }

    @Override
    protected Object createTest() throws Exception {
        targetObject = super.createTest();
        return targetObject;
    }

    @Override
    public void run(RunNotifier notifier) {
        notifier.addListener(new RunListener());
        super.run(notifier);
    }

    class RunListener extends org.junit.runner.notification.RunListener {
        @Override
        public void testStarted(Description description) throws Exception {
            if (field != null) {
                try {
                    tracingData = new SegmentStorage();
                    field.set(targetObject, tracingData);
                } catch (IllegalAccessException e) {
                }
            }
            tracingContextListener = new TracingContextListener() {
                @Override
                public void afterFinished(TraceSegment traceSegment) {
                    tracingData.addTraceSegment(traceSegment);
                }
            };

            ignoreTracerContextListener = new IgnoreTracerContextListener() {
                @Override
                public void afterFinished(IgnoredTracerContext tracerContext) {
                    tracingData.addIgnoreTraceContext(tracerContext);
                }
            };
            TracingContext.ListenerManager.add(tracingContextListener);
            IgnoredTracerContext.ListenerManager.add(ignoreTracerContextListener);
            super.testStarted(description);
        }

        @Override
        public void testFinished(Description description) throws Exception {
            super.testFinished(description);
            TracingContext.ListenerManager.remove(tracingContextListener);
            IgnoredTracerContext.ListenerManager.remove(ignoreTracerContextListener);
        }

        @Override
        public void testFailure(Failure failure) throws Exception {
            super.testFailure(failure);
            TracingContext.ListenerManager.remove(tracingContextListener);
        }
    }
}

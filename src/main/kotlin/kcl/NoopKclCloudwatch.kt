package kcl

// This class is no longer needed with KCL v2 as metrics are configured directly in the scheduler
// See KclWorkerImpl.kt where we set MetricsLevel.NONE and metricsPublishingEnabled(false)
class NoopKclCloudwatch {
    // Empty implementation as KCL v2 handles metrics configuration differently
}
package ru.eapo.stub;

public record Settings(
        String processingMode,
        int resultPublishDelayMs,
        boolean errorSimulationEnabled,
        String errorCode,
        String errorMessage,
        boolean retryable
) {
    public Settings() {
        this("STATUS_TRANSITION", 0, false, "INTERNAL_ERROR", "Test processing error", false);
    }
}

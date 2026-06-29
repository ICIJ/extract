package org.icij.extract.parser;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/** Carries PST/OST folder fan-out settings through the Tika ParseContext. */
public record PstFanoutConfig(boolean enabled, Supplier<ExecutorService> executor) {}

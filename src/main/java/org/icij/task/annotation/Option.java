package org.icij.task.annotation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Options.class)
public @interface Option {

	String name();

	String description();

	String code() default "";

	String parameter() default "yes/no";
}

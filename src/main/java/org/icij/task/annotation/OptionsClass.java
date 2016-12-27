package org.icij.task.annotation;

import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(OptionsClasses.class)
public @interface OptionsClass {

	Class<?> value();
}

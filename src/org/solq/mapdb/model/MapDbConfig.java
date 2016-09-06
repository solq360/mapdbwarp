package org.solq.mapdb.model;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface MapDbConfig {
    MapDbConfig DEFAULE = new MapDbConfig() {

	@Override
	public Class<? extends Annotation> annotationType() {
	    return MapDbConfig.class;
	}

	@Override
	public int maxSize() {
	    return 4000;
	}

	@Override
	public int exTime() {
	    return 60 * 30;
	}

	@Override
	public int checkPeriod() {
	    return 60;
	}
    };

    public int exTime();

    public int maxSize();

    public int checkPeriod();
}

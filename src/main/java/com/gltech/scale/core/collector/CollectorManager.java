package com.gltech.scale.core.collector;

import com.google.inject.Injector;
import com.gltech.scale.core.lifecycle.LifeCycle;

public interface CollectorManager extends Runnable, LifeCycle
{
	void setInjector(Injector injector);
}

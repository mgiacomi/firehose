package com.gltech.scale.core.writer;

import com.google.inject.Injector;
import com.gltech.scale.core.lifecycle.LifeCycle;

public interface StorageWriteManager extends Runnable, LifeCycle
{
	void setInjector(Injector injector);
}

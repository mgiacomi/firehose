package com.gltech.scale.core.writer;

import com.google.inject.Injector;
import com.gltech.scale.lifecycle.LifeCycle;

public interface StorageWriteManager extends LifeCycle
{
	void start();

	void setInjector(Injector injector);
}

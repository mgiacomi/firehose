package com.gltech.scale.core.coordination;

import com.gltech.scale.core.lifecycle.LifeCycle;
import com.gltech.scale.core.rope.RopeManagersByPeriod;
import org.joda.time.DateTime;

public interface RopeCoordinator extends Runnable, LifeCycle
{
	void registerWeight(boolean active, int primaries, int backups, int restedfor);

	RopeManagersByPeriod getRopeManagerPeriodMatrix(DateTime nearestPeriodCeiling);
}
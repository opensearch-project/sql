package com.wiredforcode.gradle.spawn

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Internal


class DefaultSpawnTask extends DefaultTask {

    @Internal
    String pidLockFileName = '.pid.lock'
    @Internal
    String directory = '.'

    @Internal
    File getPidFile() {
        return new File(directory, pidLockFileName)
    }
}

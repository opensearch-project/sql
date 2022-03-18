package com.wiredforcode.gradle.spawn

import org.gradle.api.Plugin
import org.gradle.api.Project


class SpawnPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.with {
            ext.SpawnProcessTask = SpawnProcessTask
            ext.KillProcessTask = KillProcessTask

            task('spawnProcess', type: SpawnProcessTask)
            task('killProcess', type: KillProcessTask)
        }
    }
}

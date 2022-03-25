package com.wiredforcode.gradle.spawn

import org.gradle.api.tasks.TaskAction

class KillProcessTask extends DefaultSpawnTask {
    @TaskAction
    void kill() {
        def pidFile = getPidFile()
        if(!pidFile.exists()) {
            logger.quiet "No server running!"
            return
        }

        def pid = pidFile.text
        def process = "kill $pid".execute()

        try {
            process.waitFor()
        } finally {
            pidFile.delete()
        }
    }
}

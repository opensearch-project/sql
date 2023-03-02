package com.wiredforcode.gradle.spawn

import org.apache.tools.ant.taskdefs.condition.Os
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
        def killCommandLine
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            killCommandLine = Arrays.asList("taskkill", "/F", "/T", "/PID", "$pid")
        } else {
            killCommandLine = Arrays.asList("kill", "$pid")
        }
        def process = killCommandLine.execute()

        try {
            process.waitFor()
        } finally {
            pidFile.delete()
        }
    }
}

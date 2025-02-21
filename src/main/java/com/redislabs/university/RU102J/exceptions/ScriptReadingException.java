package com.redislabs.university.RU102J.exceptions;

import java.io.IOException;

public class ScriptReadingException extends IOException {
    public ScriptReadingException(String message) {
        super(message);
    }

    public ScriptReadingException(String message, Throwable cause) {
        super(message, cause);
    }
}


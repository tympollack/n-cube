package com.cedarsoftware.ncube.exception

import groovy.transform.CompileStatic

@CompileStatic
class CubeSizeException extends RuntimeException
{
    CubeSizeException(String message) { super(message) }
}

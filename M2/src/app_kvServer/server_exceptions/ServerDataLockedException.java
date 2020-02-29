package server_exceptions;

import java.lang.Exception;

public class ServerDataLockedException extends Exception 
{ 
    public ServerDataLockedException(String s) 
    { 
        super(s); 
    } 
} 
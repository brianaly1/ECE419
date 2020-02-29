package server_exceptions;

import java.lang.Exception;

public class ServerNotActiveException extends Exception 
{ 
    public ServerNotActiveException(String s) 
    { 
        super(s); 
    } 
} 
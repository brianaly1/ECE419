package server_exceptions;

import java.lang.Exception;

public class NotInHashRangeException extends Exception 
{ 
    public NotInHashRangeException(String s) 
    { 
        super(s); 
    } 
} 
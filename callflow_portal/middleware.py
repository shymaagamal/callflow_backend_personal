import logging 
import os
logger = logging.getLogger('request_tracker') 

class RequestTrackerMiddleware: 
    def __init__(self, get_response): 
        self.get_response = get_response 

    def __call__(self, request): 
        # Source information 
        remote_addr = request.META.get('REMOTE_ADDR', 'unknown') 
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR', '') 
        real_ip = x_forwarded_for.split(',')[0].strip() if x_forwarded_for else remote_addr 
    
        logger.warning( 
            "REQUEST START | path=%s | method=%s | user=%s | pid=%s | source=%s | remote_addr=%s | xff=%s", 
            request.path, 
            request.method, 
            getattr(request, 'user', None), 
            os.getpid(), 
            real_ip,          # best guess of real client IP 
            remote_addr,      # direct connection IP 
            x_forwarded_for or '-' 
        ) 
    
        response = self.get_response(request) 
    
        logger.warning( 
            "REQUEST END   | path=%s | status=%s | pid=%s | source=%s", 
            request.path, response.status_code, os.getpid(), real_ip 
        ) 
        return response 
 
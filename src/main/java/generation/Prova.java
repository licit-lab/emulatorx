package generation;

import node.AreaNodexSender;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Prova {
	public static void main(String[] args){
		AreaNodexSender sender = new AreaNodexSender();
		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(sender,0,2, TimeUnit.MINUTES);
	}
}

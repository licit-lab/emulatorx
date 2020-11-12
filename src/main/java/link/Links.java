package link;

import java.util.HashMap;

public class Links {
	private HashMap<Long,Linkx> links;

	public Links() {
		this.links = new HashMap<>();
	}

	public Links(HashMap<Long,Linkx> links) {
		this.links = links;
	}

	public HashMap<Long, Linkx> getLinks() {
		return links;
	}

	public void setLinks(HashMap<Long,Linkx> links) {
		this.links = links;
	}

	public void addLink(Linkx link){
		this.links.put(link.getId(),link);
	}

	public Linkx getLink(long linkId){
		return this.links.get(linkId);
	}
}

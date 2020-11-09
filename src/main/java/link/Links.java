package link;

import java.util.HashMap;

public class Links {
	private HashMap<Long,Link> links;

	public Links() {
		this.links = new HashMap<>();
	}

	public Links(HashMap<Long,Link> links) {
		this.links = links;
	}

	public HashMap<Long, Link> getLinks() {
		return links;
	}

	public void setLinks(HashMap<Long,Link> links) {
		this.links = links;
	}

	public void addLink(Link link){
		this.links.put(link.getId(),link);
	}

	public Link getLink(long linkId){
		return this.links.get(linkId);
	}
}

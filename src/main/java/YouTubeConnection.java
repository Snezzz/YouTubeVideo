import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.*;
import com.google.common.collect.Lists;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class YouTubeConnection {

    private String PROPERTIES_FILENAME = "youtube.properties";
    public static YouTube youtube;
    private int max_results = 50;
    private List<String> ids;
    public static String apiKey;
    public Map <String,List<String>> video;

    YouTubeConnection(String [] channels) throws IOException {
        Properties properties = new Properties();
        apiKey = properties.getProperty("youtube.apikey");
        video = new HashMap<String, List<String>>();
        List<String> scopes = Lists.newArrayList("https://www.googleapis.com/auth/youtube.force-ssl");
        try {
            Credential credential = Auth.authorize(scopes, "commentthreads");
            youtube = new YouTube.Builder(Auth.HTTP_TRANSPORT, Auth.JSON_FACTORY, credential).setApplicationName("youtube-cmdline-search-sample").build();
        } catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }
        //результат запроса
        for (int i = 0; i < channels.length; i++){
            video.put(channels[i],getVideos(channels[i]));
        }

    }


    private List<String> getVideos(String id) throws IOException {
        YouTube.Channels.List channels = youtube.channels().list("id,snippet,contentDetails");
        channels.setKey(apiKey);
        channels.setId(id);
        List<PlaylistItem> playlistItemList = new ArrayList<PlaylistItem>();
        List<String> idList = new ArrayList<String>();
        //отправляем запрос на сервер
        ChannelListResponse searchResponse = channels.execute();
        List<Channel> items = searchResponse.getItems();
        Channel channel = items.get(0);
        Object contentDetails = channel.get("contentDetails");

        ObjectMapper oMapper = new ObjectMapper();
        Map<String, Object> map = oMapper.convertValue(contentDetails, Map.class);

        Object relatedPlaylists = map.get("relatedPlaylists");
        Map<String, Object> uploads = oMapper.convertValue(relatedPlaylists, Map.class);
        String uploads_id = uploads.get("uploads").toString();
        //запрос к списку видео
        YouTube.PlaylistItems.List playlistItemRequest =
                youtube.playlistItems().list("id,contentDetails");
        playlistItemRequest.setPlaylistId(uploads_id);
        playlistItemRequest.setMaxResults((long)max_results);

        playlistItemRequest.setFields(
                "items(contentDetails/videoId,contentDetails/videoPublishedAt),nextPageToken,pageInfo");
        String nextToken = "";
        int count=0;
        boolean stop=false;
        do {
            playlistItemRequest.setPageToken(nextToken);
            //получаем список из 50 видео
            PlaylistItemListResponse playlistItemResult = playlistItemRequest.execute();
            //берем их id для получения информации в дальнейшем
            Iterator<PlaylistItem> iteratorSearchResults = playlistItemResult.getItems().iterator();
            while (iteratorSearchResults.hasNext()) {
                PlaylistItem singleVideo = iteratorSearchResults.next();
                    String video_id = singleVideo.getContentDetails().getVideoId();
                    if(singleVideo.getContentDetails().getVideoPublishedAt().toString().contains("2019")){
                        continue;
                    }
                    else if(singleVideo.getContentDetails().getVideoPublishedAt().toString().contains("2017")){
                       stop = true;
                        break;
                      }
                  else
                        idList.add(video_id);
            }
            nextToken = playlistItemResult.getNextPageToken();
            if(stop){
                nextToken = null;
            }
            count+=playlistItemResult.getItems().size();
        } while (nextToken != null);

        System.out.println("всего видео у данного канала:"+count);
        return idList;
    }

    private void get_video_id(Iterator<SearchResult> iteratorSearchResults) {
        if (!iteratorSearchResults.hasNext()) {
               System.out.println(" There aren't any results for your query.");
        }
//перебираем список видео
        while (iteratorSearchResults.hasNext()) {
            SearchResult singleVideo = iteratorSearchResults.next();
            ResourceId rId = singleVideo.getId(); //id видео
            if (rId.getKind().equals("youtube#video")) {
                String id = rId.getVideoId();
                ids.add(id); //здесь берем только id для дальнейшего получения данных
            }
        }
    }

}

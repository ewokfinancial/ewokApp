from firebase import firebase
import pickle
from requests.exceptions import HTTPError, ConnectionError

HN_BASE = "https://hacker-news.firebaseio.com/v0/"

class HNScraper:
	def __init__(self):
		self.fb = firebase.FirebaseApplication(HN_BASE)

	def fetch_stories(self, startidx, start, end, file):
		"""
		startidx: int, id number of the first item to be retrieved
		start: date in UNIX time for the first item
		end: date in UNIX time for the last item
		file: str, name of the file to be written into

		Writes the stories into a pickle file and returns a list
		of all of the stories
		"""
		stories = []
		# for the first retrival i is found by trail and error
		# afterwards it is the id of the last item stored during
		# the last retrival
		i = startidx
		while True:
			print("Getting story: ", i)
			story = self.get_item(i)
			if story is None or 'time' not in story or story['type'] != 'story':
				i += 1
				continue
			if story['time'] >= start and story['time'] <= end:
				stories.append(story)
			elif story['time'] > end:
				break
			i += 1
			# save the file every 100 stories
			if i % 100 == 0:
				with open(file, 'wb') as f:
					pickle.dump(stories, f)
				print("Last dumped story: ", i -1 )
		with open(file, 'wb') as f:
				pickle.dump(stories, f)
		
		return stories

	def get_item(self, num=1):
		"""
		returns an item of type 'type' from the HN API
		"""
		while True:
			try:
				item = self.fb.get('/v0/item', num)
				break
			except HTTPError:
				print("HTTPError! Retrying!")
			except ConnectionError:
				print("ConnectionError! Retrying!")
		return item

"""
hn = HNScraper()
startind = 11142100
start_time = 1451606400
end_time = 1483228800
f = 'stories_2016.pickle'
hn.fetch_stories(startind, start_time, end_time, f)
"""
import nltk

# Define stopword lists
neg_stops = ['no',
	'nor',
	'not',
	'don',
	"don't",
	'ain',
	'aren',
	"aren't",
	'couldn',
	"couldn't",
	'didn',
	"didn't",
	'doesn',
	"doesn't",
	'hadn',
	"hadn't",
	'hasn',
	"hasn't",
	'haven',
	"haven't",
	'isn',
	"isn't",
	'mightn',
	"mightn't",
	'mustn',
	"mustn't",
	'needn',
	"needn't",
	'shan',
	"shan't",
	'shouldn',
	"shouldn't",
	'wasn',
	"wasn't",
	'weren',
	"weren't",
	"won'",
	"won't",
	'wouldn',
	"wouldn't",
	'but',
	"don'",
	"ain't"]

common_nonneg_contr = ["could've",
	"he'd",
	"he'd've",
	"he'll",
	"he's",
	"how'd",
	"how'll",
	"how's",
	"i'd",
	"i'd've",
	"i'll",
	"i'm",
	"i've",
	"it'd",
	"it'd've",
	"it'll",
	"it's",
	"let's",
	"ma'am",
	"might've",
	"must've",
	"o'clock",
	"'ow's'at",
	"she'd",
	"she'd've",
	"she'll",
	"she's",
	"should've",
	"somebody'd",
	"somebody'd've",
	"somebody'll",
	"somebody's",
	"someone'd",
	"someone'd've",
	"someone'll",
	"someone's",
	"something'd",
	"something'd've",
	"something'll",
	"something's",
	"that'll",
	"that's",
	"there'd",
	"there'd've",
	"there're",
	"there's",
	"they'd",
	"they'd've",
	"they'll",
	"they're",
	"they've",
	"'twas",
	"we'd",
	"we'd've",
	"we'll",
	"we're",
	"we've",
	"what'll",
	"what're",
	"what's",
	"what've",
	"when's",
	"where'd",
	"where's",
	"where've",
	"who'd",
	"who'd've",
	"who'll",
	"who're",
	"who's",
	"who've",
	"why'll",
	"why're",
	"why's",
	"would've",
	"y'all",
	"y'all'll",
	"y'all'd've",
	"you'd",
	"you'd've",
	"you'll",
	"you're",
	"you've"]

letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
  'u', 'v', 'w', 'x', 'y', 'z']

ranks = ['st', 'nd', 'rd', 'th']

def create_stopword_list(nltk_english = True, contractions = True, single_letters = True, rank_suffixes = True, remove_negs = True):

	# Figure out if the stopwords corpus is present
	try:
		dir(nltk.corpus.stopwords)
	except AttributeError:
		nltk.download('stopwords')

	# Assemble all the stopwords into a list
	stops = []
	if nltk_english:
		stops += nltk.corpus.stopwords.words('english')
	if contractions:
		stops += common_nonneg_contr
	if single_letters:
		stops += letters
	if rank_suffixes:
		stops += ranks
	stops += [""] + ['us'] + [''] + ["'"]

	# Remove all negative stopwords and any duplicates
	if remove_negs:
		stops = list(set(stops) - set(neg_stops))

	return stops

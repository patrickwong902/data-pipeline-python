import os

try:
    strategy = os.environ["strategy"]
    print("Using strategy: {}".format(strategy))
except KeyError:
    strategy = "local"

config = {"bronze->silver": {
    "files": [
        "NBA_Regular_Season",
        "NBA_Playoff"
    ],
    "strategy": {
        "source": strategy,
        "sink": strategy
    }
},
    "cleansed->curated": {
        "files": [],
        "strategy": {
            "source": strategy,
            "sink": strategy
        }
    },
    "dimensions": {
        "files": [],
        "strategy": {
            "sink": strategy
        }
    },
    "curated->export": {
        "files": [],
        "strategy": {
            "source": strategy,
            "sink": strategy
        }
    }
}

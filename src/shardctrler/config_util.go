package shardctrler

import (
	"6.824/util"
	"sort"
)

func initConfig() *Config {
	cfg := &Config{}
	cfg.Num = 0
	var shards [NShards]int
	cfg.Shards = shards
	for index, _ := range shards {
		shards[index] = 0
	}
	cfg.Groups = map[int][]string{}
	return cfg
}

func (cfg *Config) makeNewCopy() *Config {
	newConfig := &Config{}
	newConfig.Num = cfg.Num

	shards := cfg.Shards
	newConfig.Shards = shards

	groups := map[int][]string{}
	newConfig.Groups = groups

	for key, val := range cfg.Groups {
		var newGroup []string
		newGroup = append(newGroup, val...)
		groups[key] = newGroup
	}
	return newConfig
}

func (cfg *Config) incrementConfigNum() {
	cfg.Num++
}

func (cfg *Config) join(servers map[int][]string) *Config {
	for gid, servers := range servers {
		cfg.Groups[gid] = servers
	}
	cfg.reBalance()
	util.Debug(util.DClient, "shardctler shards after join rebalance, %s", cfg.Shards)
	return cfg
}

func (cfg *Config) leave(gIDs []int) *Config {
	//util.Debug(util.DClient, "shardctler after before rebalance, shards %s, groups %s, gIDs %s", cfg.Shards, cfg.Groups, gIDs)
	for _, leaveGid := range gIDs {
		delete(cfg.Groups, leaveGid)
		//util.Debug(util.DClient, "shardctler groups after delete map entry, shards %s, groups %s, leaveGid %s", cfg.Shards, cfg.Groups, leaveGid)
		for shard, gid := range cfg.Shards {
			if leaveGid == gid {
				cfg.Shards[shard] = 0
			}
		}
	}
	cfg.reBalance()
	//util.Debug(util.DClient, "shardctler after leave rebalance, shards %s, groups %s", cfg.Shards, cfg.Groups)
	return cfg
}

func (cfg *Config) move(shard int, gid int) *Config {
	cfg.Shards[shard] = gid
	return cfg
}

func (cfg *Config) reBalance() {
	if len(cfg.Groups) == 0 {
		return
	}
	frequency := cfg.calculateFrequency()

	// step1 assign gid == 0 shards to other shards evenly
	for shard, gid := range cfg.Shards {
		if gid == 0 {
			cfg.redistributeShardWithGid0(frequency, shard)
		}
	}

	// step2 redistribute other keys
	cfg.redistributeShardFromMaxToMin(frequency)

}

func (cfg *Config) calculateFrequency() map[int]int {
	frequency := map[int]int{} // gid -> count
	for gid, _ := range cfg.Groups {
		frequency[gid] = 0
	}
	for _, gid := range cfg.Shards {
		if gid != 0 {
			frequency[gid] = frequency[gid] + 1
		}
	}
	return frequency
}

func (cfg *Config) redistributeShardWithGid0(frequency map[int]int, shard int) {
	gidToAddShard, _ := cfg.getGidAndCountWithMinimumShard(frequency)
	cfg.Shards[shard] = gidToAddShard
	// update frequency
	frequency[gidToAddShard]++
}

func (cfg *Config) getGidAndCountWithMaximumShard(frequency map[int]int) (int, int) {
	var frequencySlice [][2]int
	for gid, count := range frequency {
		singleFrequency := [2]int{gid, count}
		frequencySlice = append(frequencySlice, singleFrequency)
	}

	sort.Sort(ByFrequencyAndGid(frequencySlice))
	return frequencySlice[len(frequencySlice)-1][0], frequencySlice[len(frequencySlice)-1][1]
}

func (cfg *Config) getGidAndCountWithMinimumShard(frequency map[int]int) (int, int) {
	var frequencySlice [][2]int
	for gid, count := range frequency {
		singleFrequency := [2]int{gid, count}
		frequencySlice = append(frequencySlice, singleFrequency)
	}

	sort.Sort(ByFrequencyAndGid(frequencySlice))
	return frequencySlice[0][0], frequencySlice[0][1]
}

type ByFrequencyAndGid [][2]int

func (a ByFrequencyAndGid) Len() int      { return len(a) }
func (a ByFrequencyAndGid) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByFrequencyAndGid) Less(i, j int) bool {
	if a[i][1] < a[j][1] {
		return true
	} else if a[i][1] > a[j][1] {
		return false
	} else {
		return a[i][0] < a[j][0]
	}
}

func (cfg *Config) redistributeShardFromMaxToMin(frequency map[int]int) {
	for {
		gidMin, countMin := cfg.getGidAndCountWithMinimumShard(frequency)
		gidMax, countMax := cfg.getGidAndCountWithMaximumShard(frequency)
		if countMax-countMin <= 1 {
			break
		}

		for shard, gid := range cfg.Shards {
			if gid == gidMax {
				cfg.Shards[shard] = gidMin
				break
			}
		}
		// update frequency
		frequency[gidMin]++
		frequency[gidMax]--
	}

}

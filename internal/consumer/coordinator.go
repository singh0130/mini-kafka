package consumer

import (
	"fmt"
	"sync"
	"time"
)

type Member struct {
	ID                 string
	LastSeen           time.Time
	AssignedPartitions []int
}

type ConsumerGroup struct {
	Name       string
	Topic      string
	Members    map[string]*Member
	Partitions []int // Total available partitiions for the topic (e.g., [0,1])
	mu         sync.Mutex
}

type Coordinator struct {
	groups map[string]*ConsumerGroup
	mu     sync.Mutex
}

func NewCoordinator() *Coordinator {
	c := &Coordinator{
		groups: make(map[string]*ConsumerGroup),
	}
	// Start a background cleaner loop for dead consumers
	go c.watchdog()
	return c
}

// JoinGroup registers a new consumer or updates an existing one
func (c *Coordinator) JoinGroup(groupName, topic, memberID string, partitions []int) []int {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupName]
	if !exists {
		group = &ConsumerGroup{
			Name:       groupName,
			Topic:      topic,
			Members:    make(map[string]*Member),
			Partitions: partitions,
		}
		c.groups[groupName] = group
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	// Register or update member
	member, exists := group.Members[memberID]
	if !exists {
		member = &Member{ID: memberID}
		group.Members[memberID] = member
		fmt.Printf("[Coordinator] Consumer '%s' joined group '%s'\n", memberID, groupName)
	}
	member.LastSeen = time.Now()

	// Trigger rebalance to redistribute partitions
	c.rebalanceLocked(group)

	return member.AssignedPartitions
}

// Heartbeat refreshes a member's lease so the coordinator knows they are alive
func (c *Coordinator) Heartbeat(groupName, memberID string) bool {
	c.mu.Lock()
	group, exists := c.groups[groupName]
	c.mu.Unlock()

	if !exists {
		return false
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	member, exists := group.Members[memberID]
	if !exists {
		return false
	}

	member.LastSeen = time.Now()
	return true
}

// rebalanceLocked distributes topic partitions evenly among active group members (Round-Robin)
func (c *Coordinator) rebalanceLocked(group *ConsumerGroup) {
	memberIDS := make([]string, 0, len(group.Members))
	for id := range group.Members {
		memberIDS = append(memberIDS, id)
	}

	if len(memberIDS) == 0 {
		return
	}

	// Clear old assignments
	for _, m := range group.Members {
		m.AssignedPartitions = []int{}
	}

	// Round-robin assignment
	for i, p := range group.Partitions {
		targetMemberID := memberIDS[i%len(memberIDS)]
		member := group.Members[targetMemberID]
		member.AssignedPartitions = append(member.AssignedPartitions, p)
	}

	fmt.Printf("[Rebalance] Group '%s' rebalanced. Active members: %d\n", group.Name, len(memberIDS))
	for _, m := range group.Members {
		fmt.Printf("[Rebalance] Consumer '%s' assigned partitions: %v\n", m.ID, m.AssignedPartitions)
	}
}

// watchdog removes dead consumers that haven't sent a heartbeat in 5 seconds
func (c *Coordinator) watchdog() {
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		for _, group := range c.groups {
			group.mu.Lock()
			changed := false
			for id, member := range group.Members {
				if time.Since(member.LastSeen) > 5*time.Second {
					fmt.Printf("[Coordinator] Consumer '%s' timed out (dead). Removing from group '%s'.\n", id, group.Name)
					delete(group.Members, id)
					changed = true
				}
			}
			if changed {
				c.rebalanceLocked(group)
			}
			group.mu.Unlock()
		}
		c.mu.Unlock()
	}
}

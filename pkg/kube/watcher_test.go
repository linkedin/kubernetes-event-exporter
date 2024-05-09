package kube

import (
	"bytes"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	testclock "k8s.io/utils/clock/testing"
)

type mockObjectMetadataProvider struct {
	cache      *lru.ARCCache
	objDeleted bool
}

func newMockObjectMetadataProvider() ObjectMetadataProvider {
	cache, err := lru.NewARC(1024)
	if err != nil {
		panic("cannot init cache: " + err.Error())
	}

	cache.Add("test", ObjectMetadata{
		Annotations: map[string]string{"test": "test"},
		Labels:      map[string]string{"test": "test"},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: "testAPI",
				Kind:       "testKind",
				Name:       "testOwner",
				UID:        "testOwner",
			},
		},
	})

	var o ObjectMetadataProvider = &mockObjectMetadataProvider{
		cache:      cache,
		objDeleted: false,
	}

	return o
}

func (o *mockObjectMetadataProvider) GetObjectMetadata(reference *corev1.ObjectReference, clientset *kubernetes.Clientset, dynClient dynamic.Interface, metricsStore *metrics.Store) (ObjectMetadata, error) {
	if o.objDeleted {
		return ObjectMetadata{}, errors.NewNotFound(schema.GroupResource{}, "")
	}

	val, _ := o.cache.Get("test")
	return val.(ObjectMetadata), nil
}

var _ ObjectMetadataProvider = &mockObjectMetadataProvider{}

func newMockEventWatcher(MaxEventAgeSeconds int64, metricsStore *metrics.Store) *EventWatcher {
	watcher := &EventWatcher{
		objectMetadataCache: newMockObjectMetadataProvider(),
		maxEventAgeSeconds:  time.Second * time.Duration(MaxEventAgeSeconds),
		fn:                  func(event *EnhancedEvent) {},
		metricsStore:        metricsStore,
	}
	return watcher
}

func TestEventWatcher_EventAge_whenEventCreatedBeforeStartup(t *testing.T) {
	// should not discard events as old as 300s=5m
	var MaxEventAgeSeconds int64 = 300
	metricsStore := metrics.NewMetricsStore("test_")
	ew := newMockEventWatcher(MaxEventAgeSeconds, metricsStore)
	output := &bytes.Buffer{}
	log.Logger = log.Logger.Output(output)

	// event is 3m before stratup time -> expect silently dropped
	startup := time.Now().Add(-10 * time.Minute)
	ew.setStartUpTime(startup)
	event1 := corev1.Event{
		LastTimestamp: metav1.Time{Time: startup.Add(-3 * time.Minute)},
	}

	// event is 3m before stratup time -> expect silently dropped
	assert.True(t, ew.isEventDiscarded(&event1))
	assert.NotContains(t, output.String(), "Event discarded as being older then maxEventAgeSeconds")
	ew.onEvent(&event1)
	assert.NotContains(t, output.String(), "Received event")
	assert.Equal(t, float64(0), testutil.ToFloat64(metricsStore.EventsProcessed))

	event2 := corev1.Event{
		EventTime: metav1.MicroTime{Time: startup.Add(-3 * time.Minute)},
	}

	assert.True(t, ew.isEventDiscarded(&event2))
	assert.NotContains(t, output.String(), "Event discarded as being older then maxEventAgeSeconds")
	ew.onEvent(&event2)
	assert.NotContains(t, output.String(), "Received event")
	assert.Equal(t, float64(0), testutil.ToFloat64(metricsStore.EventsProcessed))

	// event is 3m before stratup time -> expect silently dropped
	event3 := corev1.Event{
		LastTimestamp: metav1.Time{Time: startup.Add(-3 * time.Minute)},
		EventTime:     metav1.MicroTime{Time: startup.Add(-3 * time.Minute)},
	}

	assert.True(t, ew.isEventDiscarded(&event3))
	assert.NotContains(t, output.String(), "Event discarded as being older then maxEventAgeSeconds")
	ew.onEvent(&event3)
	assert.NotContains(t, output.String(), "Received event")
	assert.Equal(t, float64(0), testutil.ToFloat64(metricsStore.EventsProcessed))

	metrics.DestroyMetricsStore(metricsStore)
}

func TestEventWatcher_EventAge_whenEventCreatedAfterStartupAndBeforeMaxAge(t *testing.T) {
	// should not discard events as old as 300s=5m
	var MaxEventAgeSeconds int64 = 300
	metricsStore := metrics.NewMetricsStore("test_")
	ew := newMockEventWatcher(MaxEventAgeSeconds, metricsStore)
	output := &bytes.Buffer{}
	log.Logger = log.Logger.Output(output)

	startup := time.Now().Add(-10 * time.Minute)
	ew.setStartUpTime(startup)

	t.Run("last timetamp is empty", func(t *testing.T) {
		// creation time is 8m after startup time (2m in max age) -> expect processed
		event := corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Time{Time: startup.Add(8 * time.Minute)},
			},
			InvolvedObject: corev1.ObjectReference{
				UID:  "test",
				Name: "test-1",
			},
		}

		assert.False(t, ew.isEventDiscarded(&event))
		assert.NotContains(t, output.String(), "Event discarded as being older then maxEventAgeSeconds")
		ew.onEvent(&event)
		assert.Contains(t, output.String(), "test-1")
		assert.Contains(t, output.String(), "Received event")
		assert.Equal(t, float64(1), testutil.ToFloat64(metricsStore.EventsProcessed))
	})

	t.Run("last timetamp is after creation timestamp", func(t *testing.T) {
		// event is 8m after stratup time (2m after max age) -> expect processed
		event := corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Time{Time: startup},
			},
			LastTimestamp: metav1.Time{Time: startup.Add(9 * time.Minute)},
			InvolvedObject: corev1.ObjectReference{
				UID:  "test",
				Name: "test-2",
			},
		}

		assert.False(t, ew.isEventDiscarded(&event))
		assert.NotContains(t, output.String(), "Event discarded as being older then maxEventAgeSeconds")
		ew.onEvent(&event)
		assert.Contains(t, output.String(), "test-2")
		assert.Contains(t, output.String(), "Received event")
		assert.Equal(t, float64(2), testutil.ToFloat64(metricsStore.EventsProcessed))
	})

	metrics.DestroyMetricsStore(metricsStore)
}

func TestEventWatcher_EventAge_whenEventCreatedAfterStartupAndAfterMaxAge(t *testing.T) {
	// should not discard events as old as 300s=5m
	var MaxEventAgeSeconds int64 = 300
	metricsStore := metrics.NewMetricsStore("test_")
	ew := newMockEventWatcher(MaxEventAgeSeconds, metricsStore)
	output := &bytes.Buffer{}
	log.Logger = log.Logger.Output(output)

	// event is 3m after stratup time (and 2m after max age) -> expect dropped with warn
	startup := time.Now().Add(-10 * time.Minute)
	ew.setStartUpTime(startup)

	t.Run("last timestamp is empty", func(t *testing.T) {
		event := corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "event1",
				CreationTimestamp: metav1.Time{Time: startup.Add(3 * time.Minute)},
			},
		}
		assert.True(t, ew.isEventDiscarded(&event))
		assert.Contains(t, output.String(), "event1")
		assert.Contains(t, output.String(), "Event discarded as being older than maxEventAgeSeconds")
		ew.onEvent(&event)
		assert.NotContains(t, output.String(), "Received event")
		assert.Equal(t, float64(0), testutil.ToFloat64(metricsStore.EventsProcessed))
	})

	t.Run("last timestamp is after creation timestamp", func(t *testing.T) {
		event := corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "event2",
				CreationTimestamp: metav1.Time{Time: startup.Add(3 * time.Minute)},
			},
			LastTimestamp: metav1.Time{Time: startup.Add(4 * time.Minute)},
		}
		assert.True(t, ew.isEventDiscarded(&event))
		assert.Contains(t, output.String(), "event2")
		assert.Contains(t, output.String(), "Event discarded as being older than maxEventAgeSeconds")
		ew.onEvent(&event)
		assert.NotContains(t, output.String(), "Received event")
		assert.Equal(t, float64(0), testutil.ToFloat64(metricsStore.EventsProcessed))
	})

	metrics.DestroyMetricsStore(metricsStore)
}

func TestOnEvent_WithObjectMetadata(t *testing.T) {
	metricsStore := metrics.NewMetricsStore("test_")
	defer metrics.DestroyMetricsStore(metricsStore)
	ew := newMockEventWatcher(300, metricsStore)

	event := EnhancedEvent{}
	ew.fn = func(e *EnhancedEvent) {
		event = *e
	}

	startup := time.Now().Add(-10 * time.Minute)
	ew.setStartUpTime(startup)
	event1 := corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "event1",
			CreationTimestamp: metav1.Time{Time: startup.Add(8 * time.Minute)},
		},
		InvolvedObject: corev1.ObjectReference{
			UID:  "test",
			Name: "test-1",
		},
	}
	ew.onEvent(&event1)

	require.Equal(t, types.UID("test"), event.InvolvedObject.UID)
	require.Equal(t, "test-1", event.InvolvedObject.Name)
	require.Equal(t, map[string]string{"test": "test"}, event.InvolvedObject.Annotations)
	require.Equal(t, map[string]string{"test": "test"}, event.InvolvedObject.Labels)
	require.Equal(t, []metav1.OwnerReference{
		{
			APIVersion: "testAPI",
			Kind:       "testKind",
			Name:       "testOwner",
			UID:        "testOwner",
		},
	}, event.InvolvedObject.OwnerReferences)
}

func TestOnEvent_DeletedObjects(t *testing.T) {
	metricsStore := metrics.NewMetricsStore("test_")
	defer metrics.DestroyMetricsStore(metricsStore)
	ew := newMockEventWatcher(300, metricsStore)
	ew.objectMetadataCache.(*mockObjectMetadataProvider).objDeleted = true

	event := EnhancedEvent{}
	ew.fn = func(e *EnhancedEvent) {
		event = *e
	}

	startup := time.Now().Add(-10 * time.Minute)
	ew.setStartUpTime(startup)
	event1 := corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "event1",
			CreationTimestamp: metav1.Time{Time: startup.Add(8 * time.Minute)},
		},
		InvolvedObject: corev1.ObjectReference{
			UID:  "test",
			Name: "test-1",
		},
	}

	ew.onEvent(&event1)

	require.Equal(t, types.UID("test"), event.InvolvedObject.UID)
	require.Equal(t, "test-1", event.InvolvedObject.Name)
	require.Equal(t, true, event.InvolvedObject.Deleted)
	require.Equal(t, map[string]string(nil), event.InvolvedObject.Annotations)
	require.Equal(t, map[string]string(nil), event.InvolvedObject.Labels)
	require.Equal(t, []metav1.OwnerReference(nil), event.InvolvedObject.OwnerReferences)
}

func TestGetEventAge(t *testing.T) {
	testTime := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	fakeClock := testclock.NewFakePassiveClock(testTime)
	testCases := map[string]struct {
		event                             corev1.Event
		expectedTimeUsedForAgeCalculation time.Time
		expectedAge                       time.Duration
	}{
		"last timestamp is after creation timestamp": {
			event: corev1.Event{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.Time{Time: testTime.Add(-5 * time.Minute)},
				},
				LastTimestamp: metav1.Time{Time: testTime.Add(-3 * time.Minute)},
			},
			expectedTimeUsedForAgeCalculation: testTime.Add(-3 * time.Minute),
			expectedAge:                       3 * time.Minute,
		},
		"creation timestamp is after last timestamp": {
			event: corev1.Event{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.Time{Time: testTime.Add(-2 * time.Minute)},
				},
				LastTimestamp: metav1.Time{Time: testTime.Add(-4 * time.Minute)},
			},
			expectedTimeUsedForAgeCalculation: testTime.Add(-2 * time.Minute),
			expectedAge:                       2 * time.Minute,
		},
		"last timestamp is empty": {
			event: corev1.Event{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.Time{Time: testTime},
				},
			},
			expectedTimeUsedForAgeCalculation: testTime,
			expectedAge:                       0,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			age, timeUsedForAgeCalculation := getEventAge(&tc.event, fakeClock)
			require.Equal(t, tc.expectedAge, age)
			require.Equal(t, tc.expectedTimeUsedForAgeCalculation, timeUsedForAgeCalculation)
		})
	}
}

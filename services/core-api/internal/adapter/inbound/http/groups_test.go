package httpadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

const (
	groupTestSecret = "a sufficiently long test signing secret 123456"
	groupTestUser   = "11111111-1111-4111-8111-111111111111"
	groupTestID     = "22222222-2222-4222-8222-222222222222"
	groupOtherID    = "33333333-3333-4333-8333-333333333333"
)

type groupHTTPRepo struct {
	*sessionRepoStub
	chats         []app.GroupChat
	operationErr  error
	createdBy     string
	createdName   string
	createdMember []string
	lastOperation string
	lastActor     string
	lastGroup     string
	lastMembers   []string
	lastTarget    string
	lastName      *string
	lastPicture   *string
	lastAdmin     bool
}

func (r *groupHTTPRepo) ListGroupChats(context.Context, string) ([]app.GroupChat, error) {
	return r.chats, r.operationErr
}
func (r *groupHTTPRepo) CreateGroup(_ context.Context, creator, name string, members []string) (string, error) {
	r.createdBy, r.createdName, r.createdMember = creator, name, members
	return "44444444-4444-4444-8444-444444444444", r.operationErr
}
func (r *groupHTTPRepo) UpdateGroup(_ context.Context, actor, group string, name, picture *string) error {
	r.lastOperation, r.lastActor, r.lastGroup, r.lastName, r.lastPicture = "update", actor, group, name, picture
	return r.operationErr
}
func (r *groupHTTPRepo) AddGroupMembers(_ context.Context, actor, group string, members []string) error {
	r.lastOperation, r.lastActor, r.lastGroup, r.lastMembers = "add-members", actor, group, members
	return r.operationErr
}
func (r *groupHTTPRepo) RemoveGroupMember(_ context.Context, actor, group, member string) error {
	r.lastOperation, r.lastActor, r.lastGroup, r.lastTarget = "remove-member", actor, group, member
	return r.operationErr
}
func (r *groupHTTPRepo) SetGroupAdmins(_ context.Context, actor, group string, members []string, admin bool) error {
	r.lastOperation, r.lastActor, r.lastGroup, r.lastMembers, r.lastAdmin = "set-admins", actor, group, members, admin
	return r.operationErr
}

func groupTestServer(repo *groupHTTPRepo) (*Server, string) {
	now := time.Unix(1_800_000_000, 0)
	claims := sessiontoken.Claims{SessionID: "sid", UserID: groupTestUser, DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}
	token, _ := sessiontoken.Sign(claims, []byte(groupTestSecret))
	service := app.New(repo, &sessionCacheStub{status: sessiontoken.StatusActive}, app.Config{Secret: []byte(groupTestSecret), Now: func() time.Time { return now }})
	return New(service, testLogger()), token
}

func callGroupAPI(server *Server, token, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	return recorder
}

func TestListGroupChatsReturnsAuthenticatedUsersGroups(t *testing.T) {
	updated := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	name, picture := "study group", "media-1"
	repo := &groupHTTPRepo{sessionRepoStub: &sessionRepoStub{}, chats: []app.GroupChat{{ID: groupTestID, Name: name, ProfilePic: &picture, UpdatedAt: updated, Members: []app.GroupMember{{UserID: groupTestID, PhoneNumber: "+15550000001", DisplayName: "Abdu"}, {UserID: groupOtherID, PhoneNumber: "+15550000002"}}}}}
	server, token := groupTestServer(repo)
	recorder := callGroupAPI(server, token, http.MethodGet, "/v1/chats/groups", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var chats []app.GroupChat
	if err := json.Unmarshal(recorder.Body.Bytes(), &chats); err != nil {
		t.Fatal(err)
	}
	if len(chats) != 1 || chats[0].ID != groupTestID || chats[0].Name != name || *chats[0].ProfilePic != picture || !chats[0].UpdatedAt.Equal(updated) || len(chats[0].Members) != 2 || chats[0].Members[0].DisplayName != "Abdu" || chats[0].Members[1].UserID != groupOtherID || chats[0].Members[1].PhoneNumber != "+15550000002" {
		t.Fatalf("group chats=%+v", chats)
	}
	if got := callGroupAPI(server, "", http.MethodGet, "/v1/chats/groups", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d, want 401", got)
	}
}

func TestLookupUserByPhoneRequiresAuthAndReturnsPhoneAndID(t *testing.T) {
	repo := &groupHTTPRepo{sessionRepoStub: &sessionRepoStub{}}
	server, token := groupTestServer(repo)
	response := callGroupAPI(server, token, http.MethodGet, "/v1/users/lookup?phone=%2B15550000002", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["userId"] != "lookup-user" || result["phoneNumber"] != "+15550000002" {
		t.Fatalf("lookup result=%v", result)
	}
	if got := callGroupAPI(server, "", http.MethodGet, "/v1/users/lookup?phone=%2B15550000002", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d, want 401", got)
	}
	if got := callGroupAPI(server, token, http.MethodGet, "/v1/users/lookup?phone=123", "").Code; got != http.StatusBadRequest {
		t.Fatalf("invalid phone status=%d, want 400", got)
	}
}

func TestLookupUserByPhoneIsRateLimited(t *testing.T) {
	repo := &groupHTTPRepo{sessionRepoStub: &sessionRepoStub{}}
	server, token := groupTestServer(repo)
	for i := 0; i < 10; i++ {
		response := callGroupAPI(server, token, http.MethodGet, "/v1/users/lookup?phone=%2B15550000002", "")
		if response.Code != http.StatusOK {
			t.Fatalf("lookup %d status=%d body=%s", i+1, response.Code, response.Body.String())
		}
	}
	response := callGroupAPI(server, token, http.MethodGet, "/v1/users/lookup?phone=%2B15550000002", "")
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") == "" {
		t.Fatalf("rate limited status=%d retry-after=%q", response.Code, response.Header().Get("Retry-After"))
	}
}

func TestGroupManagementEndpoints(t *testing.T) {
	repo := &groupHTTPRepo{sessionRepoStub: &sessionRepoStub{}}
	server, token := groupTestServer(repo)
	memberIDs := `"memberUserIds":["` + groupOtherID + `"]`
	tests := []struct {
		method, path, body, operation string
		status                        int
	}{
		{http.MethodPost, "/v1/groups", `{"name":"learning",` + memberIDs + `}`, "create", http.StatusCreated},
		{http.MethodPatch, "/v1/groups/" + groupTestID, `{"name":"renamed","profilePic":"pic"}`, "update", http.StatusNoContent},
		{http.MethodPost, "/v1/groups/" + groupTestID + "/members", `{"userIds":["` + groupOtherID + `"]}`, "add-members", http.StatusNoContent},
		{http.MethodDelete, "/v1/groups/" + groupTestID + "/members/" + groupOtherID, "", "remove-member", http.StatusNoContent},
		{http.MethodPost, "/v1/groups/" + groupTestID + "/admins", `{"userIds":["` + groupOtherID + `"]}`, "set-admins", http.StatusNoContent},
		{http.MethodDelete, "/v1/groups/" + groupTestID + "/admins/" + groupOtherID, "", "set-admins", http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.operation+" "+test.method, func(t *testing.T) {
			recorder := callGroupAPI(server, token, test.method, test.path, test.body)
			if recorder.Code != test.status {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if test.method != http.MethodPost || test.path != "/v1/groups" {
				if repo.lastOperation != test.operation || repo.lastActor != groupTestUser || repo.lastGroup != groupTestID {
					t.Fatalf("operation=%q actor=%q group=%q", repo.lastOperation, repo.lastActor, repo.lastGroup)
				}
			}
		})
	}
	if repo.createdBy != groupTestUser || repo.createdName != "learning" || len(repo.createdMember) != 1 || repo.createdMember[0] != groupOtherID {
		t.Fatalf("creation args creator=%q name=%q members=%v", repo.createdBy, repo.createdName, repo.createdMember)
	}
	if repo.lastAdmin || repo.lastMembers[0] != groupOtherID {
		t.Fatalf("demote request admin=%v members=%v", repo.lastAdmin, repo.lastMembers)
	}
}

func TestGroupManagementMapsPermissionErrorsAndRejectsInvalidBodies(t *testing.T) {
	repo := &groupHTTPRepo{sessionRepoStub: &sessionRepoStub{}, operationErr: app.ErrForbidden}
	server, token := groupTestServer(repo)
	if got := callGroupAPI(server, token, http.MethodPatch, "/v1/groups/"+groupTestID, `{"name":"new"}`).Code; got != http.StatusForbidden {
		t.Fatalf("forbidden status=%d, want 403", got)
	}
	repo.operationErr = nil
	if got := callGroupAPI(server, token, http.MethodPost, "/v1/groups", `{"name":"","memberUserIds":[]}`).Code; got != http.StatusBadRequest {
		t.Fatalf("invalid body status=%d, want 400", got)
	}
	if got := callGroupAPI(server, token, http.MethodPatch, "/v1/groups/"+groupTestID, `{"name":"   "}`).Code; got != http.StatusBadRequest {
		t.Fatalf("blank metadata name status=%d, want 400", got)
	}
}

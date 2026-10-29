package app

import "regexp"

var e164PhonePattern = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

// IsE164Phone checks the E.164 wire format without country-specific validation.
func IsE164Phone(phone string) bool {
	return e164PhonePattern.MatchString(phone)
}

import re


EMAIL_PATTERN = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+"
    r"@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+"
)

SSN_PATTERN = re.compile(
    r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])"
)

CARD_PATTERN = re.compile(
    r"(?<![0-9])(?:[0-9][ -]?){12,18}[0-9](?![0-9])"
)


def redact_text(text: str) -> str:
    """Redact supported email, SSN, and card-like patterns."""
    text = EMAIL_PATTERN.sub("[REDACTED]", text)
    text = SSN_PATTERN.sub("[REDACTED]", text)
    text = CARD_PATTERN.sub("[REDACTED]", text)
    return text


class TokenRedactor:
    """Buffer tokens and possible space-separated numeric sequences.

    If pending text exceeds the limit, redact it and suppress the
    rest of that line. This bounds memory and fails closed.
    """

    def __init__(self, max_buffer_chars: int = 512):
        if max_buffer_chars < 1:
            raise ValueError("max_buffer_chars must be positive")

        self.max_buffer_chars = max_buffer_chars
        self.pending = ""
        self.held = ""
        self.discarding_line = False

    @property
    def buffered_chars(self) -> int:
        return len(self.pending) + len(self.held)

    def feed(self, chunk: str) -> str:
        """Accept a text delta and return text ready to release."""
        output = []

        for character in chunk:
            if self.discarding_line:
                if character in "\r\n":
                    output.append(character)
                    self.discarding_line = False
                continue

            # Keep retained text within the configured limit.
            if self.buffered_chars + 1 > self.max_buffer_chars:
                output.append("[REDACTED]")
                self.pending = ""
                self.held = ""

                if character in "\r\n":
                    output.append(character)
                else:
                    self.discarding_line = True

                continue

            if not character.isspace():
                self.pending += character
                continue

            combined = self.held + self.pending

            if character in "\r\n":
                # Supported patterns do not span line breaks.
                output.append(redact_text(combined))
                output.append(character)
                self.pending = ""
                self.held = ""

            elif self.pending and self.pending[-1] in "0123456789-":
                # A card number may continue after a space.
                self.held = combined + character
                self.pending = ""

            elif not self.pending and self.held:
                # Preserve additional whitespace while deciding.
                self.held += character

            else:
                output.append(redact_text(combined))
                output.append(character)
                self.pending = ""
                self.held = ""

        return "".join(output)

    def finish(self) -> str:
        """Flush remaining text when the provider stream ends."""
        result = (
            ""
            if self.discarding_line
            else redact_text(self.held + self.pending)
        )

        self.pending = ""
        self.held = ""
        self.discarding_line = False
        return result
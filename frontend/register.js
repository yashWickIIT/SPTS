function toErrorText(payload) {
  if (payload == null) return "Registration failed.";

  if (typeof payload === "string") {
    return payload;
  }

  if (Array.isArray(payload)) {
    const messages = payload
      .map((item) => {
        if (typeof item === "string") return item;
        if (item && typeof item.msg === "string") {
          const field = Array.isArray(item.loc) ? item.loc[item.loc.length - 1] : null;
          if (field === "username") return `Username: ${item.msg}`;
          if (field === "password") return `Password: ${item.msg}`;
          if (field === "role") return `Role: ${item.msg}`;
          return item.msg;
        }
        return null;
      })
      .filter(Boolean);

    return messages.length ? messages.join("; ") : "Registration failed.";
  }

  if (typeof payload === "object") {
    if (typeof payload.detail === "string") return payload.detail;
    if (Array.isArray(payload.detail)) return toErrorText(payload.detail);
    if (payload.detail && typeof payload.detail.msg === "string") return payload.detail.msg;
    if (typeof payload.msg === "string") return payload.msg;
  }

  return "Registration failed.";
}


async function getResponseErrorMessage(response) {
  let errorData = null;
  let rawErrorText = "";

  try {
    errorData = await response.json();
  } catch (parseError) {
    console.warn("Failed to parse registration error payload:", parseError);
    try {
      rawErrorText = await response.text();
    } catch (textError) {
      console.warn("Failed to read registration error text:", textError);
    }
  }

  const parsedMessage = toErrorText(errorData);
  if (parsedMessage && parsedMessage !== "Registration failed.") {
    return parsedMessage;
  }

  if (response.status === 429) {
    return "Too many registration attempts. Please wait a minute and try again.";
  }

  const plainText = rawErrorText?.trim();
  if (plainText) {
    return plainText;
  }

  return `Registration failed (${response.status}).`;
}


async function register() {
  const usernameInput = document.getElementById("username").value.trim();
  const passwordInput = document.getElementById("password").value;
  const roleInput = document.getElementById("role").value;
  const errorMsg = document.getElementById("error-msg");
  const successMsg = document.getElementById("success-msg");
  const registerBtn = document.getElementById("registerBtn");

  errorMsg.style.display = "none";
  successMsg.style.display = "none";

  if (!usernameInput || !passwordInput) {
    errorMsg.textContent = "Please enter both username and password.";
    errorMsg.style.display = "block";
    return;
  }
  
  if (passwordInput.length < 4) {
      errorMsg.textContent = "Password must be at least 4 characters long.";
      errorMsg.style.display = "block";
      return;
  }

  registerBtn.textContent = "Registering...";
  registerBtn.disabled = true;

  try {
    const response = await fetch("/register", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        username: usernameInput,
        password: passwordInput,
        role: roleInput,
      }),
    });

    if (response.ok) {
        successMsg.textContent = "Registration successful! You can now log in.";
        successMsg.style.display = "block";
        document.getElementById("username").value = "";
        document.getElementById("password").value = "";
        document.getElementById("role").value = "analyst";
    } else {
      errorMsg.textContent = await getResponseErrorMessage(response);

      errorMsg.style.display = "block";
    }
  } catch (error) {
    console.error("Registration error:", error);
    errorMsg.textContent = "An error occurred during registration. Please try again.";
    errorMsg.style.display = "block";
  } finally {
    registerBtn.textContent = "Register";
    registerBtn.disabled = false;
  }
}

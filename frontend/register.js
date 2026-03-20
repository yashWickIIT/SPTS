async function register() {
  const usernameInput = document.getElementById("username").value;
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
      const errorData = await response.json();
      errorMsg.textContent = errorData.detail || "Registration failed.";
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

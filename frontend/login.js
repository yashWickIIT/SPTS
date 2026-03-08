async function login() {
  const usernameInput = document.getElementById("username").value;
  const passwordInput = document.getElementById("password").value;
  const errorMsg = document.getElementById("error-msg");
  const loginBtn = document.getElementById("loginBtn");

  if (!usernameInput || !passwordInput) {
    errorMsg.textContent = "Please enter both username and password.";
    errorMsg.style.display = "block";
    return;
  }

  errorMsg.style.display = "none";
  loginBtn.textContent = "Logging in...";
  loginBtn.disabled = true;

  try {
    const formData = new URLSearchParams();
    formData.append("username", usernameInput);
    formData.append("password", passwordInput);

    const response = await fetch("/token", {
      method: "POST",
      body: formData,
    });

    if (response.ok) {
      const data = await response.json();
      localStorage.setItem("spts_token", data.access_token);
      window.location.href = "/static/index.html";
    } else {
      const errorData = await response.json();
      errorMsg.textContent = errorData.detail || "Login failed. Please check your credentials.";
      errorMsg.style.display = "block";
    }
  } catch (error) {
    console.error("Login error:", error);
    errorMsg.textContent = "An error occurred during login. Please try again.";
    errorMsg.style.display = "block";
  } finally {
    loginBtn.textContent = "Log In";
    loginBtn.disabled = false;
  }
}

// Clear any existing token on load of the login page
window.addEventListener('DOMContentLoaded', () => {
    localStorage.removeItem("spts_token");
});

const emailInput = document.getElementById("login-email");
const passwordInput = document.getElementById("login-password");
const errorBox = document.getElementById("login-error");
const submitBtn = document.getElementById("login-submit");

async function doLogin() {
    errorBox.classList.add("d-none");
    submitBtn.disabled = true;
    submitBtn.textContent = "Signing in...";

    try {
        const response = await fetch("/auth/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                email: emailInput.value.trim(),
                password: passwordInput.value,
            }),
        });

        if (!response.ok) {
            const data = await response.json().catch(() => ({}));
            throw new Error(data.detail || "Login failed.");
        }

        window.location.href = "/";
    } catch (err) {
        errorBox.textContent = err.message;
        errorBox.classList.remove("d-none");
        submitBtn.disabled = false;
        submitBtn.textContent = "Sign In";
    }
}

submitBtn.addEventListener("click", doLogin);
passwordInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") doLogin();
});
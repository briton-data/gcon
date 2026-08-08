const tokenInput = document.getElementById("reset-token");
const passwordInput = document.getElementById("reset-password");
const passwordConfirmInput = document.getElementById("reset-password-confirm");
const errorBox = document.getElementById("reset-error");
const successBox = document.getElementById("reset-success");
const submitBtn = document.getElementById("reset-submit");

async function doResetPassword() {
    errorBox.classList.add("d-none");
    successBox.classList.add("d-none");

    if (!tokenInput.value) {
        errorBox.textContent = "This reset link is missing its token. Request a new one.";
        errorBox.classList.remove("d-none");
        return;
    }
    if (passwordInput.value !== passwordConfirmInput.value) {
        errorBox.textContent = "Passwords do not match.";
        errorBox.classList.remove("d-none");
        return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = "Resetting...";

    try {
        const response = await fetch("/auth/reset-password", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                token: tokenInput.value,
                new_password: passwordInput.value,
            }),
        });

        if (!response.ok) {
            const data = await response.json().catch(() => ({}));
            throw new Error(data.detail || "Could not reset password.");
        }

        successBox.textContent = "Password reset. You can now sign in.";
        successBox.classList.remove("d-none");
        submitBtn.textContent = "Done";
        setTimeout(() => { window.location.href = "/login"; }, 1500);
    } catch (err) {
        errorBox.textContent = err.message;
        errorBox.classList.remove("d-none");
        submitBtn.disabled = false;
        submitBtn.textContent = "Reset password";
    }
}

submitBtn.addEventListener("click", doResetPassword);

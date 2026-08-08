const emailInput = document.getElementById("forgot-email");
const errorBox = document.getElementById("forgot-error");
const successBox = document.getElementById("forgot-success");
const submitBtn = document.getElementById("forgot-submit");

async function doForgotPassword() {
    errorBox.classList.add("d-none");
    successBox.classList.add("d-none");
    submitBtn.disabled = true;
    submitBtn.textContent = "Sending...";

    try {
        const response = await fetch("/auth/forgot-password", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email: emailInput.value.trim() }),
        });

        const data = await response.json().catch(() => ({}));
        if (!response.ok) {
            throw new Error(data.detail || "Something went wrong.");
        }

        successBox.textContent = data.message || "If that email has an account, a reset link has been sent.";
        successBox.classList.remove("d-none");
        submitBtn.textContent = "Sent";
    } catch (err) {
        errorBox.textContent = err.message;
        errorBox.classList.remove("d-none");
        submitBtn.disabled = false;
        submitBtn.textContent = "Send reset link";
    }
}

submitBtn.addEventListener("click", doForgotPassword);

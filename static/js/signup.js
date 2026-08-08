const nameInput = document.getElementById("signup-name");
const emailInput = document.getElementById("signup-email");
const roleInput = document.getElementById("signup-role");
const passwordInput = document.getElementById("signup-password");
const passwordConfirmInput = document.getElementById("signup-password-confirm");
const errorBox = document.getElementById("signup-error");
const successBox = document.getElementById("signup-success");
const submitBtn = document.getElementById("signup-submit");

async function doSignup() {
    errorBox.classList.add("d-none");
    successBox.classList.add("d-none");

    if (passwordInput.value !== passwordConfirmInput.value) {
        errorBox.textContent = "Passwords do not match.";
        errorBox.classList.remove("d-none");
        return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = "Creating account...";

    try {
        const response = await fetch("/auth/signup", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                name: nameInput.value.trim(),
                email: emailInput.value.trim(),
                role: roleInput.value,
                password: passwordInput.value,
            }),
        });

        if (!response.ok) {
            const data = await response.json().catch(() => ({}));
            throw new Error(data.detail || "Could not create account.");
        }

        successBox.textContent = "Account created. An admin needs to approve it before you can sign in.";
        successBox.classList.remove("d-none");
        submitBtn.textContent = "Account created";
    } catch (err) {
        errorBox.textContent = err.message;
        errorBox.classList.remove("d-none");
        submitBtn.disabled = false;
        submitBtn.textContent = "Create account";
    }
}

submitBtn.addEventListener("click", doSignup);
